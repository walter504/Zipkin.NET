using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Tracing.Core;
using Zipkin.Core;
using Zipkin.Core.Json;
using Zipkin.UI.Web.ViewModels;

using WebUtil = Zipkin.UI.Web.Helpers.Util;

namespace Zipkin.UI.Web.Controllers
{
    public class TracesController : Controller
    {
        [Route("traces/{id}")]
        public ActionResult Index(string id)
        {
            var traceId = Util.HexToLong(id);
            var client = new RestClient("http://localhost:9411");
            var trace = client.Execute<List<JsonSpan>>(new RestRequest(string.Format("/api/v1/trace/{0}", traceId))).Data
                .Select(js => js.Invert()).ToList();
            if (trace.Count() == 0)
            {
                return HttpNotFound();
            }

            var traceTimestamp = trace.First().timestamp ?? 0L;
            var traceDuration = Trace.Duration(trace) ?? 0L;
            var spanDepths = TraceSummary.ToSpanDepths(trace);
            var spanMap = WebUtil.GetIdToSpanMap(trace);

            var rootSpans = WebUtil.GetRootSpans(trace);
            var spans = new List<Dictionary<string, object>>();
            foreach (var rootSpan in rootSpans)
            {
                foreach (var span in SpanNode.Create(rootSpan, trace).ToSpans())
                {
                    var spanStartTs = span.timestamp ?? traceTimestamp;
                    int depth;
                    if (!spanDepths.TryGetValue(span.id, out depth))
                    {
                        depth = 1;
                    }
                    var width = span.duration.HasValue ? 100 * (double)span.duration.Value / (double)traceDuration : 0.0;

                    var binaryAnnotations = new List<JsonBinaryAnnotation>();
                    span.binaryAnnotations.ToList().ForEach(ba =>
                    {
                        if (Constants.CoreAddress.Contains(ba.key))
                        {
                            if (ba.endpoint != null)
                            {
                                binaryAnnotations.Add(ToHostAndPort(Constants.CoreAnnotationNames[ba.key], ba.endpoint));
                            }
                        }
                        else
                        {
                            var jsonAnno = new JsonBinaryAnnotation(ba);
                            if (Constants.CoreAnnotationNames.ContainsKey(ba.key))
                            {
                                jsonAnno.key = Constants.CoreAnnotationNames[ba.key];
                            }
                            binaryAnnotations.Add(jsonAnno);
                        }
                    });
                    span.binaryAnnotations.Where(ba => ba.key == Constants.LocalComponent).ToList().ForEach(ba =>
                    {
                        if (ba.endpoint != null)
                        {
                            binaryAnnotations.Add(ToHostAndPort("Local Address", ba.endpoint));
                        }
                    });

                    spans.Add(new Dictionary<string, object>() 
                    { 
                        {"spanId", Util.LongToHex(span.id)},
                        {"parentId", span.parentId.HasValue && spanMap.ContainsKey(span.parentId.Value) ? Util.LongToHex(span.parentId.Value) : string.Empty },
                        {"spanName", span.name},
                        {"serviceNames", string.Join(",", span.ServiceNames)},
                        {"serviceName", span.ServiceName},
                        {"duration", span.duration},
                        {"durationStr", WebUtil.FormatDurtion(span.duration ?? 0)},
                        {"left", ((float)(spanStartTs - traceTimestamp) / (float) traceDuration) * 100},
                        {"width", width < 0.1 ? 0.1 : width},
                        {"depth", (depth + 1) * 5},
                        {"depthClass", (depth - 1) % 6},
                        {"children", Util.LongToHex(span.id)},
                        {"annotations",  span.annotations.Select(a => new Dictionary<string, object>() 
                            {
                                {"isCore", Constants.CoreAnnotations.Contains(a.value)},
                                {"left", span.duration.HasValue ? 100 * (float)(a.timestamp - spanStartTs) / (float)span.duration.Value : 0},
                                {"endpoint", a.endpoint == null ? string.Empty : string.Format("{0}:{1}", a.endpoint.GetHostAddress(), a.endpoint.GetUnsignedPort()) },
                                {"value", WebUtil.GetAnnotationName(a.value)},
                                {"timestamp", a.timestamp},
                                {"relativeTime", WebUtil.FormatDurtion(a.timestamp - traceTimestamp)},
                                {"serviceName", a.endpoint == null ? string.Empty : a.endpoint.serviceName},
                                {"width", 8}
                            }).ToList()},
                        {"binaryAnnotations", binaryAnnotations}
                    });
                }
            }

            var serviceDurations = new List<MustacheServiceDuration>();
            var summary = TraceSummary.Create(trace);
            if (summary != null)
            {
                serviceDurations = summary.SpanTimestamps.GroupBy(sts => sts.Name).Select(g => new MustacheServiceDuration()
                {
                    name = g.Key,
                    count = g.Count(),
                    max = g.Max(st => st.Duration / 1000)
                }).ToList();
            }
            var i = 0;
            var timeMarkers = new double[] { 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 }
                .Select(p => new Dictionary<string, string>() 
                { 
                    { "index", (i++).ToString() }, 
                    { "time", WebUtil.FormatDurtion((long)(traceDuration * p)) } 
                }).ToList();

            var timeMarkersBackup = timeMarkers.Select(m => m).ToList();
            var spansBackup = spans.Select(s => s).ToList();

            ViewBag.duration = WebUtil.FormatDurtion(traceDuration);
            ViewBag.services = serviceDurations.Count;
            ViewBag.depth = spanDepths.Values.Max();
            ViewBag.totalSpans = spans.Count;
            ViewBag.serviceCounts = serviceDurations.OrderBy(sd => sd.name);
            ViewBag.timeMarkers = timeMarkers;
            ViewBag.timeMarkersBackup = timeMarkersBackup;
            ViewBag.spans = spans;
            ViewBag.spansBackup = spansBackup;
            return View();
        }

        private JsonBinaryAnnotation ToHostAndPort(string key, Endpoint endpoint)
        {
            return new JsonBinaryAnnotation()
            {
                key = key,
                value = string.Format("{0}:{1}", endpoint.GetHostAddress(), endpoint.GetUnsignedPort()),
                endpoint = endpoint == null ? null : new JsonEndpoint(endpoint)
            };

        }
    }
}