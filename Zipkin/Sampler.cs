using System;
using Zipkin.Internal;

namespace Zipkin
{
    public abstract class Sampler
    {
        /** Returns true if the trace ID should be recorded. */
        public abstract bool IsSampled(long traceId);

        public static Sampler Create(float rate)
        {
            if (rate == 0.0F) return NEVER_SAMPLE;
            if (rate == 1.0F) return ALWAYS_SAMPLE;
            return new ProbabilisticSampler(rate);
        }
        static readonly NeverSampler NEVER_SAMPLE = new NeverSampler();
        static readonly AlwaysSampler ALWAYS_SAMPLE = new AlwaysSampler();

        class NeverSampler : Sampler
        {
            public override bool IsSampled(long traceId)
            {
                return false;
            }
            public override string ToString()
            {
                return "NEVER_SAMPLE";
            }
        }
        
        class AlwaysSampler : Sampler
        {
            public override bool IsSampled(long traceId)
            {
              return true;
            }
            public override string ToString()
            {
              return "ALWAYS_SAMPLE";
            }
        }
    }

    /**
     * Accepts a percentage of trace ids by comparing their absolute value against a boundary. eg
     * {@code iSampled == abs(traceId) < boundary}
     *
     * <p>While idempotent, this implementation's sample rate won't exactly match the input rate
     * because trace ids are not perfectly distributed across 64bits. For example, tests have shown an
     * error rate of 3% when trace ids are {@link java.util.Random#nextLong random}.
     */
    public sealed class ProbabilisticSampler : Sampler
    {

        /** {@link #isSampled(long)} returns true when abs(traceId) < boundary */
        private readonly long boundary;

        public ProbabilisticSampler(float rate)
        {
            Ensure.ArgumentAssert(rate > 0 && rate < 1, "rate should be between 0 and 1: was %s", rate);
            this.boundary = (long)(long.MaxValue * rate); // safe cast as less than 1
        }

        public override bool IsSampled(long traceId)
        {
            // The absolute value of Long.MIN_VALUE is larger than a long, so Math.abs returns identity.
            // This converts to MAX_VALUE to avoid always dropping when traceId == Long.MIN_VALUE
            long t = traceId == long.MinValue ? long.MaxValue : Math.Abs(traceId);
            return t < boundary;
        }

        public override string ToString()
        {
            return "ProbabilisticSampler(" + boundary + ")";
        }
    }
}
