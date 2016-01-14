using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core.Internal
{
    public class ApplyTimestampAndDuration
    {
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
