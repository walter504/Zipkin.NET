using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Zipkin.Core;
using Zipkin.Core.Json;

namespace Zipkin.UI.Web.Controllers
{
    public class HomeController : Controller
    {
        // GET: Home
        public ActionResult Index(
            string serviceName = null,
            string spanName = null,
            long? endTs = null,
            string annotationQuery = null,
            int? limit = null,
            int? minDuration = null)
        {
            ViewBag.serviceName = serviceName;
            ViewBag.endTs = (endTs ?? Util.CurrentTimeMilliSeconds()).ToString();
            ViewBag.annotationQuery = annotationQuery ?? string.Empty;
            ViewBag.limit = (limit ?? 10).ToString();
            ViewBag.minDuration = minDuration.HasValue ? minDuration.Value.ToString() : string.Empty;
            ViewBag.Services = new List<object>();
            ViewBag.Spans = new List<string>();

            var spans = new List<string>();
            var traces = new List<TraceSummary>();
            if (!string.IsNullOrEmpty(serviceName))
            {
                var client = new RestClient("http://localhost:9411");
                spans = client.Execute<List<string>>(new RestRequest(string.Format("/api/v1/spans?serviceName={0}", serviceName))).Data;

                var tracesReq = new RestRequest("/api/v1/traces");
                foreach (var key in Request.QueryString.AllKeys)
                {
                    tracesReq.AddParameter(key, Request.QueryString[key]);
                }
                var response = client.Execute<List<List<JsonSpan>>>(tracesReq);
                traces = response.Data
                    .Select(t => TraceSummary.Create(t.Select(js => js.Invert())))
                    .Where(ts => ts != null).ToList();
            }
            if (spans.Count != 0 && traces.Count != 0)
            {
                ViewBag.Spans = spans.Select(s => new Dictionary<string, string> { { "name", s}, { "selected", s == spanName ? "selected" : string.Empty} });
                ViewBag.queryResults = TraceSummaryToMustache(serviceName, traces);
            }
            return View();
        }

        private Dictionary<string, object> TraceSummaryToMustache(string serviceName, IList<TraceSummary> ts)
        {
            var maxDuration = ts.Max(t => t.Duration / 1000);
            var traces = ts.Select(t =>
            {
                var duration = t.Duration / 1000;
                var groupedSpanTimestamps = t.SpanTimestamps.GroupBy(s => s.Name);
                var serviceDurations = groupedSpanTimestamps.Select(g =>
                    new MustacheServiceDuration() { name = g.Key, count = g.Count(), max = g.Max(st => st.Duration / 1000) });

                long? serviceTime = null;
                if (!string.IsNullOrEmpty(serviceName))
                {
                    var timestamps = groupedSpanTimestamps.FirstOrDefault(g => g.Key == serviceName);
                    if (timestamps != null)
                    {
                        serviceTime = TotalServiceTime(timestamps);
                    }
                }
                return new MustacheTraceSummary()
                {
                    traceId = t.TraceId,
                    startTs = Util.FromUnixTimeSeconds(t.Timestamp / 1000).ToString(),
                    timestamp = t.Timestamp,
                    duration = duration,
                    durationStr = TimeSpan.FromMilliseconds(duration).ToString(),
                    servicePercentage = serviceTime.HasValue ? (int)(100 * (float)serviceTime.Value / (float)t.Duration) : 0,
                    spanCount = t.SpanTimestamps.Count(),
                    serviceDurations = serviceDurations,
                    width = (int)(100 * (float)duration / (float)maxDuration)
                };
            }).OrderBy(t => t.duration).Reverse();
            return new Dictionary<string, object>()
            {
                {"traces", traces},
                {"count", traces.Count()}
            };
        }

        private long TotalServiceTime(IEnumerable<SpanTimestamp> stamps, long acc = 0)
        {
            if (stamps.Count() == 0)
            {
                return acc;
            }
            else
            {
                var ts = stamps.Aggregate((curMin, s) => (curMin == null || s.Timestamp < curMin.Timestamp) ? s : curMin);
                var current = new List<SpanTimestamp>();
                var next = new List<SpanTimestamp>();
                foreach (var stamp in stamps)
                {
                    if (stamp.Timestamp >= ts.Timestamp && stamp.EndTs <= ts.EndTs)
                    {
                        current.Add(stamp);
                    }
                    else
                    {
                        next.Add(stamp);
                    }
                }
                var endTs = current.Max(s => s.EndTs);
                return TotalServiceTime(next, acc + (endTs - ts.Timestamp));
            }
        }

        public class MustacheServiceDuration
        {
            public string name { get; set; }
            public int count { get; set; }
            public long max { get; set; }
        }

        public class MustacheTraceSummary
        {
            public string traceId { get; set; }
            public string startTs { get; set; }
            public long timestamp { get; set; }
            public long duration { get; set; }
            public string durationStr { get; set; }
            public int servicePercentage { get; set; }
            public int spanCount { get; set; }
            public IEnumerable<MustacheServiceDuration> serviceDurations { get; set; }
            public int width { get; set; }
        }
    }
}