using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public sealed class QueryRequest
    {
        /** Mandatory {@link io.zipkin.Endpoint#serviceName} and constrains all other parameters. */
        public readonly string serviceName;

        public readonly string spanName;

        /**
         * Include traces whose {@link io.zipkin.Span#annotations} include a value in this set.
         *
         * <p/> This is an AND condition against the set, as well against {@link #binaryAnnotations}
         */
        public readonly IList<string> annotations;

        /**
         * Include traces whose {@link io.zipkin.Span#binaryAnnotations} include a string whose key and
         * value are an entry in this set.
         *
         * <p/> This is an AND condition against the set, as well against {@link #annotations}
         */
        public readonly IDictionary<string, string> binaryAnnotations;

        /**
         * Only return traces whose {@link io.zipkin.Span#duration} is greater than or equal to
         * minDuration microseconds.
         */
        public readonly long? minDuration;

        /**
         * Only return traces whose {@link io.zipkin.Span#duration} is less than or equal to maxDuration
         * microseconds. Only valid with {@link #minDuration}.
         */
        public readonly long? maxDuration;

        /**
         * Only return traces where all {@link io.zipkin.Span#timestamp} are at or before this time in
         * epoch milliseconds. Defaults to current time.
         */
        public readonly long endTs;

        /**
         * Only return traces where all {@link io.zipkin.Span#timestamp} are at or after (endTs -
         * lookback) in milliseconds. Defaults to endTs.
         */
        public readonly long lookback;

        /** Maximum number of traces to return. Defaults to 10 */
        public readonly int limit;

        public QueryRequest(
            string serviceName,
            string spanName,
            IList<string> annotations,
            IDictionary<string, string> binaryAnnotations,
            long? minDuration,
            long? maxDuration,
            long? endTs,
            long? lookback,
            int? limit)
        {
            Ensure.ArgumentAssert(serviceName != null && !string.IsNullOrEmpty(serviceName), "serviceName was empty");
            Ensure.ArgumentAssert(spanName == null || !string.IsNullOrEmpty(spanName), "spanName was empty");
            Ensure.ArgumentAssert(endTs > 0, "endTs should be positive, in epoch microseconds: was %d", endTs);
            Ensure.ArgumentAssert(limit > 0, "limit should be positive: was %d", limit);
            this.serviceName = serviceName.ToLower();
            this.spanName = spanName != null ? spanName.ToLower() : null;
            this.annotations = annotations;
            foreach (string annotation in annotations)
            {
                Ensure.ArgumentAssert(!string.IsNullOrEmpty(annotation), "annotation was empty");
            }
            this.binaryAnnotations = binaryAnnotations;
            foreach (var pair in binaryAnnotations)
            {
                Ensure.ArgumentAssert(!string.IsNullOrEmpty(pair.Key), "binary annotation key was empty");
                Ensure.ArgumentAssert(!string.IsNullOrEmpty(pair.Value), "binary annotation value was empty");
            }
            this.minDuration = minDuration;
            this.maxDuration = maxDuration;
            this.endTs = endTs ?? Util.CurrentTimeMilliSeconds();
            this.lookback = Math.Min(lookback ?? this.endTs, this.endTs);
            this.limit = limit ?? 10;
        }

        public override string ToString()
        {
            return "QueryRequest{"
                + "serviceName=" + serviceName + ", "
                + "spanName=" + spanName + ", "
                + "annotations=" + annotations + ", "
                + "binaryAnnotations=" + binaryAnnotations + ", "
                + "minDuration=" + minDuration + ", "
                + "maxDuration=" + maxDuration + ", "
                + "endTs=" + endTs + ", "
                + "lookback=" + lookback + ", "
                + "limit=" + limit
                + "}";
        }

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is QueryRequest)
            {
                QueryRequest that = (QueryRequest)o;
                return (this.serviceName.Equals(that.serviceName))
                    && ((this.spanName == null) ? (that.spanName == null) : this.spanName.Equals(that.spanName))
                    && ((this.annotations == null) ? (that.annotations == null) : this.annotations.Equals(that.annotations))
                    && ((this.binaryAnnotations == null) ? (that.binaryAnnotations == null) : this.binaryAnnotations.Equals(that.binaryAnnotations))
                    && ((this.minDuration == null) ? (that.minDuration == null) : this.minDuration.Equals(that.minDuration))
                    && ((this.maxDuration == null) ? (that.maxDuration == null) : this.maxDuration.Equals(that.maxDuration))
                    && (this.endTs == that.endTs)
                    && (this.lookback == that.lookback)
                    && (this.limit == that.limit);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= serviceName.GetHashCode();
            h *= 1000003;
            h ^= (spanName == null) ? 0 : spanName.GetHashCode();
            h *= 1000003;
            h ^= (annotations == null) ? 0 : annotations.GetHashCode();
            h *= 1000003;
            h ^= (binaryAnnotations == null) ? 0 : binaryAnnotations.GetHashCode();
            h *= 1000003;
            h ^= (minDuration == null) ? 0 : minDuration.GetHashCode();
            h *= 1000003;
            h ^= (maxDuration == null) ? 0 : maxDuration.GetHashCode();
            h *= 1000003;
            h ^= (int)((endTs >> 32) ^ endTs);
            h *= 1000003;
            h ^= (int)((lookback >> 32) ^ lookback);
            h *= 1000003;
            h ^= limit;
            return h;
        }
    }
}
