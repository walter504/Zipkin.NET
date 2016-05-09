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

        public static Span Apply(Span s)
        {
            if ((!s.timestamp.HasValue || !s.duration.HasValue) && 0 != s.annotations.Count)
            {
                long? ts = s.timestamp;
                long? dur = s.duration;
                ts = ts ?? s.annotations[0].timestamp;
                if (!dur.HasValue)
                {
                    long lastTs = s.annotations[s.annotations.Count - 1].timestamp;
                    if (ts != lastTs)
                    {
                        dur = lastTs - ts;
                    }
                }
                s.timestamp = ts;
                s.duration = dur;
            }
            return s;
        }
    }
}
