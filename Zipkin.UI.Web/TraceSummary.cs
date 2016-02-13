using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tracing.Core;
using Zipkin.Core;

namespace Zipkin.UI.Web
{
    public class TraceSummary
    {
        public string TraceId { get; set; }
        public long Timestamp { get; set; }
        public long Duration { get; set; }
        public IEnumerable<SpanTimestamp> SpanTimestamps { get; set; }
        public IEnumerable<Endpoint> Endpoints { get; set; }

        public static TraceSummary Create(IEnumerable<Span> trace)
        {
            if (trace.Count() != 0 && trace.First().timestamp.HasValue)
            {
                return new TraceSummary()
                {
                    TraceId = Util.LongToHex(trace.First().traceId),
                    Timestamp = trace.First().timestamp.Value,
                    Duration = Trace.Duration(trace) ?? 0L,
                    SpanTimestamps = GetSpanTimestamps(trace),
                    Endpoints = trace.SelectMany(t => t.Endpoints).Distinct()
                };
            }
            return null;  
        }

        private static IEnumerable<SpanTimestamp> GetSpanTimestamps(IEnumerable<Span> spans)
        {
            var results = new List<SpanTimestamp>();
            foreach (var span in spans)
            {
                if (span.timestamp.HasValue && span.duration.HasValue)
                {
                    foreach (var serviceName in span.ServiceNames)
                    {
                        results.Add(new SpanTimestamp(serviceName, span.timestamp.Value, span.duration.Value));
                    }
                }
            }
            return results;
        }
    }

    public class SpanTimestamp
    {
        public string Name { get; set; }
        public long Timestamp { get; set; }
        public long Duration { get; set; }
        public long EndTs
        {
            get
            {
                return this.Timestamp + Duration;
            }
        }
        public SpanTimestamp(string name, long timestamp, long duration)
        {
            this.Name = name;
            this.Timestamp = timestamp;
            this.Duration = duration;
        }
    }
}