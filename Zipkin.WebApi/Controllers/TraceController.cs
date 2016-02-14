using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Zipkin.Core;
using Zipkin.Core.Json;

namespace Zipkin.WebApi.Controllers
{
    [RoutePrefix("api/v1")]
    public class TraceController : ApiController
    {
        const int defaultLookback = 3600 * 24 * 7 * 1000; // 7 days in millis
        const int defaultLimit = 10;

        private readonly ISpanStore spanStore;
        private readonly SpanWriter spanWriter;

        public TraceController(ISpanStore spanStore, SpanWriter spanWriter)
        {
            this.spanStore = spanStore;
            this.spanWriter = spanWriter;
        }

        [Route("dependencies")]
        public IHttpActionResult GetDependencies(long endTs, long? lookback = null)
        {
            return Ok(spanStore.GetDependencies(endTs, lookback ?? defaultLookback));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Route("services")]
        public IHttpActionResult GetServiceNames()
        {
            return Ok(spanStore.GetServiceNames());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Route("spans")]
        public IHttpActionResult GetSpanNames(string serviceName)
        {
            return Ok(spanStore.GetSpanNames(serviceName));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="spans"></param>
        [Route("spans")]
        public void PostSpans([FromBody]IEnumerable<Span> spans)
        {
            spanWriter.Write(spanStore, spans);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serviceName"></param>
        /// <param name="spanName"></param>
        /// <param name="annotationQuery"></param>
        /// <param name="minDuration"></param>
        /// <param name="maxDuration"></param>
        /// <param name="endT"></param>
        /// <param name="lookback"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        [Route("traces")]
        public IHttpActionResult GetTraces(
            string serviceName,
            string spanName = "all",
            string annotationQuery = null,
            long? minDuration = null,
            long? maxDuration = null,
            long? endTs = null,
            long lookback = defaultLookback,
            int limit = defaultLimit)
        {
            if (!endTs.HasValue)
            {
                endTs = Util.ToUnixTimeMilliseconds(DateTime.Now);
            }
            List<string> annotations = new List<string>();
            Dictionary<string, string> binaryAnnotations = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(annotationQuery))
            {
                foreach (var ann in annotationQuery.Split(new string[] { " and " }, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (ann.IndexOf('=') == -1)
                    {
                        annotations.Add(ann);
                    }
                    else
                    {
                        string[] keyValue = ann.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                        if (keyValue.Length < 2)
                        {
                            annotations.Add(ann);
                        }
                        binaryAnnotations.Add(keyValue[0], keyValue[1]);
                    }
                }
            }
            QueryRequest request = new QueryRequest(
                serviceName,
                spanName == "all" ? null : spanName,
                annotations,
                binaryAnnotations,
                minDuration,
                maxDuration,
                endTs,
                lookback,
                limit);
            return Ok(spanStore.GetTraces(request).Select(t => t.Select(s => new JsonSpan(s))));
        }

        [Route("trace/{traceId}")]
        public IHttpActionResult GetTrace(long traceId)
        {
            var traces = spanStore.GetTracesByIds(new long[]{traceId});
            if (traces.Count() == 0)
            {
                return NotFound();
            }
            return Ok(traces.First().Select(s => new JsonSpan(s)));
        }
        
    }
}
