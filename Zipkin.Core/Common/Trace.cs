using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Core;

namespace Tracing.Core
{
    public class Trace
    {
        public static long? Duration(IEnumerable<Span> spans)
        {
            var timestamps = spans.SelectMany(s => s.timestamp.HasValue 
                ? (s.duration.HasValue 
                    ? new List<long>() { s.timestamp.Value, s.timestamp.Value + s.duration.Value }
                    : new List<long>() { s.timestamp.Value })
                : new List<long>()).ToList();
            timestamps.Sort();
            if (timestamps.Count != 0)
            {
                var first = timestamps[0];
                var last = timestamps[timestamps.Count - 1];
                if (last != first)
                {
                    return last - first;
                }
            }
            return null;
        }
    }
}
