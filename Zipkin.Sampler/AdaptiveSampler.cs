using System;
using System.Threading;

namespace Zipkin.Sampler
{
    public class AdaptiveSampler : IDisposable
    {
        /// <summary>
        /// Dynamic sampling estimator settings.
        /// </summary>
        private SamplingPercentageEstimatorSettings settings;

        /// <summary>
        /// Average telemetry item counter.
        /// </summary>
        private ExponentialMovingAverageCounter itemCount;

        /// <summary>
        /// Evaluation timer.
        /// </summary>
        private Timer evaluationTimer;

        /// <summary>
        /// Current evaluation interval.
        /// </summary>
        private TimeSpan evaluationInterval;

        /// <summary>
        /// Current sampling rate.
        /// </summary>
        private int currenSamplingRate;

        /// <summary>
        /// Last date and time sampling percentage was changed.
        /// </summary>
        private DateTimeOffset samplingPercentageLastChangeDateTime;

        /// <summary>
        /// Initializes a new instance of the <see cref="SamplingPercentageEstimatorTelemetryProcessor"/> class.
        /// <param name="next">Next TelemetryProcessor in call chain.</param>
        /// </summary>
        public AdaptiveSampler()
            : this(new SamplingPercentageEstimatorSettings())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SamplingPercentageEstimatorTelemetryProcessor"/> class.
        /// <param name="settings">Dynamic sampling estimator settings.</param>
        /// <param name="callback">Callback to invoke every time sampling percentage is evaluated.</param>
        /// <param name="next">Next TelemetryProcessor in call chain.</param>
        /// </summary>
        public AdaptiveSampler(SamplingPercentageEstimatorSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException("settings");
            }
            this.settings = settings;

            this.currenSamplingRate = settings.EffectiveInitialSamplingRate;

            this.itemCount = new ExponentialMovingAverageCounter(settings.EffectiveMovingAverageRatio);

            this.samplingPercentageLastChangeDateTime = DateTimeOffset.UtcNow;

            // set evaluation interval to default value if it is negative or zero
            this.evaluationInterval = this.settings.EffectiveEvaluationInterval;

            // set up timer to run math to estimate sampling percentage
            this.evaluationTimer = new Timer(
                this.EstimateSamplingPercentage, 
                null,
                this.evaluationInterval,
                this.evaluationInterval);
        }

        public bool IsSampled(long traceId)
        {
            // The absolute value of Long.MIN_VALUE is larger than a long, so Math.abs returns identity.
            // This converts to MAX_VALUE to avoid always dropping when traceId == Long.MIN_VALUE
            long t = traceId == long.MinValue ? long.MaxValue : Math.Abs(traceId);
            var isSampled = t < long.MaxValue * (this.currenSamplingRate / 100F);
            if (isSampled)
            {
                // increment post-samplin telemetry item counter
                this.itemCount.Increment();
            }
            return isSampled;
        }

        /// <summary>
        /// Gets or sets initial sampling percentage applied at the start
        /// of the process to dynamically vary the percentage.
        /// </summary>
        public double InitialSamplingPercentage
        {
            get
            {
                return this.settings.InitialSamplingPercentage;
            }

            set
            {
                // note: 'initial' percentage will affect sampling even 
                // if it was running for a while
                this.settings.InitialSamplingPercentage = value;
            }
        }

        /// <summary>
        /// Gets or sets maximum rate of telemetry items per second
        /// dynamic sampling will try to adhere to.
        /// </summary>
        public double MaxTelemetryItemsPerSecond
        {
            get
            {
                return this.settings.MaxTelemetryItemsPerSecond;
            }

            set
            {
                this.settings.MaxTelemetryItemsPerSecond = value;
            }
        }

        /// <summary>
        /// Gets or sets minimum sampling percentage that can be set 
        /// by the dynamic sampling percentage algorithm.
        /// </summary>
        public double MinSamplingPercentage
        {
            get
            {
                return this.settings.MinSamplingPercentage;
            }

            set
            {
                this.settings.MinSamplingPercentage = value;
            }
        }

        /// <summary>
        /// Gets or sets maximum sampling percentage that can be set 
        /// by the dynamic sampling percentage algorithm.
        /// </summary>
        public double MaxSamplingPercentage
        {
            get
            {
                return this.settings.MaxSamplingPercentage;
            }

            set
            {
                this.settings.MaxSamplingPercentage = value;
            }
        }

        /// <summary>
        /// Gets or sets duration of the sampling percentage evaluation interval.
        /// </summary>
        public TimeSpan EvaluationInterval
        {
            get
            {
                return this.settings.EvaluationInterval;
            }

            set
            {
                this.settings.EvaluationInterval = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how long to not to decrease
        /// sampling percentage after last change to prevent excessive fluctuation.
        /// </summary>
        public TimeSpan SamplingPercentageDecreaseTimeout
        {
            get
            {
                return this.settings.SamplingPercentageDecreaseTimeout;
            }

            set
            {
                this.settings.SamplingPercentageDecreaseTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how long to not to increase
        /// sampling percentage after last change to prevent excessive fluctuation.
        /// </summary>
        public TimeSpan SamplingPercentageIncreaseTimeout
        {
            get
            {
                return this.settings.SamplingPercentageIncreaseTimeout;
            }

            set
            {
                this.settings.SamplingPercentageIncreaseTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets exponential moving average ratio (factor) applied
        /// during calculation of rate of telemetry items produced by the application.
        /// </summary>
        public double MovingAverageRatio
        {
            get
            {
                return this.settings.MovingAverageRatio;
            }

            set
            {
                this.settings.MovingAverageRatio = value;
            }
        }

        /// <summary>
        /// Disposes the object.
        /// </summary>
        public void Dispose()
        {
            if (this.evaluationTimer != null)
            {
                this.evaluationTimer.Dispose();
                this.evaluationTimer = null;
            }
        }

        /// <summary>
        /// Checks to see if exponential moving average has changed.
        /// </summary>
        /// <param name="running">Currently running value of moving average.</param>
        /// <param name="current">Value set in the algorithm parameters.</param>
        /// <returns>True if moving average value changed.</returns>
        private static bool MovingAverageCoefficientChanged(double running, double current)
        {
            const double Precision = 1E-12;

            return (running < current - Precision) || (running > current + Precision);
        }

        /// <summary>
        /// Callback for sampling percentage evaluation timer.
        /// </summary>
        /// <param name="state">Timer state.</param>
        private void EstimateSamplingPercentage(object state)
        {
            // get observed after-sampling eps
            double observedEps = this.itemCount.StartNewInterval() / this.evaluationInterval.TotalSeconds;

            // we see events post sampling, so get pre-sampling eps
            double beforeSamplingEps = observedEps * this.currenSamplingRate;

            // caclulate suggested sampling rate
            int suggestedSamplingRate = (int)Math.Ceiling(beforeSamplingEps / this.settings.EffectiveMaxTelemetryItemsPerSecond);

            // adjust suggested rate so that it fits between min and max configured
            if (suggestedSamplingRate > this.settings.EffectiveMaxSamplingRate)
            {
                suggestedSamplingRate = this.settings.EffectiveMaxSamplingRate;
            }

            if (suggestedSamplingRate < this.settings.EffectiveMinSamplingRate)
            {
                suggestedSamplingRate = this.settings.EffectiveMinSamplingRate;
            }

            // see if evaluation interval was changed and apply change
            if (this.evaluationInterval != this.settings.EffectiveEvaluationInterval)
            {
                this.evaluationInterval = this.settings.EffectiveEvaluationInterval;
                this.evaluationTimer.Change(this.evaluationInterval, this.evaluationInterval);
            }

            // check to see if sampling rate needs changes
            bool samplingPercentageChangeNeeded = suggestedSamplingRate != this.currenSamplingRate;

            if (samplingPercentageChangeNeeded)
            {
                // check to see if enough time passed since last sampling % change
                if ((DateTimeOffset.UtcNow - this.samplingPercentageLastChangeDateTime) <
                    (suggestedSamplingRate > this.currenSamplingRate
                        ? this.settings.EffectiveSamplingPercentageDecreaseTimeout
                        : this.settings.EffectiveSamplingPercentageIncreaseTimeout))
                {
                    samplingPercentageChangeNeeded = false;
                }
            }

            if (samplingPercentageChangeNeeded)
            { 
                // apply sampling perfcentage change
                this.samplingPercentageLastChangeDateTime = DateTimeOffset.UtcNow;
                this.currenSamplingRate = suggestedSamplingRate;
            }

            if (samplingPercentageChangeNeeded || 
                MovingAverageCoefficientChanged(this.itemCount.Coefficient, this.settings.EffectiveMovingAverageRatio))
            {
                // since we're observing event count post sampling and we're about
                // to change sampling rate or change coefficient, reset counter
                this.itemCount = new ExponentialMovingAverageCounter(this.settings.EffectiveMovingAverageRatio);
            }
        }
    }
}
