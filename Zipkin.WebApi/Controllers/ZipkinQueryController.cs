using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Zipkin.Internal;
using Zipkin.Json;
using Zipkin.Storage;

namespace Zipkin.WebApi.Controllers
{
    [RoutePrefix("api/v1")]
    public class ZipkinQueryController : ApiController
    {
        const int defaultLookback = 3600 * 24 * 7 * 1000; // 7 days in millis
        const int defaultLimit = 10;

        private readonly ISpanStore spanStore;
        private readonly ZipkinSpanWriter spanWriter;
        private readonly IDependencyStore dependencyStore;

        public ZipkinQueryController(ISpanStore spanStore, IDependencyStore dependencyStore, ZipkinSpanWriter spanWriter)
        {
            this.spanStore = spanStore;
            this.dependencyStore = dependencyStore;
            this.spanWriter = spanWriter;
        }

        [HttpGet]
        [Route("dependencies")]
        public async Task<IHttpActionResult> GetDependencies(long endTs, long? lookback = null)
        {
            return Ok(await dependencyStore.GetDependencies(endTs, lookback ?? defaultLookback));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Route("services")]
        public async Task<IHttpActionResult> GetServiceNames()
        {
            return Ok(await spanStore.GetServiceNames());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Route("spans")]
        public async Task<IHttpActionResult> GetSpanNames(string serviceName)
        {
            return Ok(await spanStore.GetSpanNames(serviceName));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="spans"></param>
        [Route("spans")]
        public async Task<IHttpActionResult> PostSpans()
        {
            var bytes = await Request.Content.ReadAsByteArrayAsync();
            List<Span> spans = null;
            var contentType = HttpContext.Current.Request.ContentType;
            if (contentType.StartsWith("application/json"))
            {
                var jsons = Jil.JSON.Deserialize<List<JsonSpan>>(Encoding.UTF8.GetString(bytes));
                spans = jsons.Select(js => js.Invert()).ToList();
            }
            else
            {
                var codec = Codec.Get(contentType);
                if (codec != null)
                {
                    spans = codec.ReadSpans(bytes);
                }
            }
            if (spans == null)
            {
                return StatusCode(HttpStatusCode.UnsupportedMediaType);
            }
            await spanWriter.Write(spanStore, spans);
            return Ok();
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
        public async Task<IHttpActionResult> GetTraces(
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
                        if (keyValue.Length <= 1)
                        {
                            annotations.Add(ann);
                        }
                        else
                        {
                            binaryAnnotations.Add(keyValue[0], keyValue[1]);
                        }
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
            var result = await spanStore.GetTraces(request);
            return Ok(result.Select(t => t.Select(s => new JsonSpan(s))));
        }

        [Route("trace/{id}")]
        public async Task<IHttpActionResult> GetTrace(long id)
        {
            var trace = await spanStore.GetTrace(id);
            if (trace == null)
            {
                return NotFound();
            }
            return Ok(trace.Select(s => new JsonSpan(s)));
        }
    }
}
