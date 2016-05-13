using System;
using System.Collections.Generic;
using System.Linq;

namespace Zipkin.Adjuster
{
    public class ApplyTimestampAndDuration
    {
        public static List<Span> Apply(IEnumerable<Span> spans)
        {
            var newSpans = spans.Select(Apply).Where(s => s.timestamp.HasValue).ToList();
            newSpans.Sort();
            return newSpans;
        }

        public static Span Apply(Span span)
        {
            // Don't overwrite authoritatively set timestamp and duration!
            if (span.timestamp != null && span.duration != null)
            {
                return span;
            }

            // Only calculate span.timestamp and duration on complete spans. This avoids
            // persisting an inaccurate timestamp due to a late arriving annotation.
            if (span.annotations.Count < 2)
            {
                return span;
            }

            // For spans that core client annotations, the distance between "cs" and "cr" should be the
            // authoritative duration. We are special-casing this to avoid setting incorrect duration
            // when there's skew between the client and the server.
            long first = span.annotations.First().timestamp;
            long last = span.annotations.Last().timestamp;
            foreach (var annotation in span.annotations)
            {
                if (annotation.value == Constants.ClientSend)
                {
                    first = annotation.timestamp;
                }
                else if (annotation.value == Constants.ClientRecv)
                {
                    last = annotation.timestamp;
                }
            }
            long ts = span.timestamp ?? first;
            long? dur = span.duration.HasValue ? span.duration : (last == first ? null : new Nullable<long>(last - first));
            return span.ToBuilder().Timestamp(ts).Duration(dur).Build();
        }
    }
}
