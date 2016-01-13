using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Tracing.Core;

namespace Tracing.WebApi.Controllers
{
    [Route("api/v1")]
    public class TraceController : ApiController
    {
        const int defaultLookback = 86400000; // 7 days in millis

        private readonly ISpanStore spanStore;
        private readonly SpanWriter spanWriter;

        public TraceController(ISpanStore spanStore, SpanWriter spanWriter)
        {
            this.spanStore = spanStore;
            this.spanWriter = spanWriter;
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
            long minDuration = 0,
            long maxDuration = 0,
            long endT = 0,
            long lookback = defaultLookback,
            int limit = 0)
        {
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
                endT,
                lookback,
                limit);
            return Ok(spanStore.GetTraces(request));
        }

        [Route("trace/{traceId}")]
        public IHttpActionResult GetTrace(long traceId)
        {
            List<List<Span>> traces = spanStore.GetTracesByIds(new long[]{traceId});
            if (traces.Count == 0)
            {
                return NotFound();
            }
            return Ok(traces.First());
        }
        
    }
}
