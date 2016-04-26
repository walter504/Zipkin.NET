using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Zipkin.Storage;

namespace Zipkin.WebApi
{
    public class ZipkinSpanWriter
    {
        public Sampler sampler { get; set; }

        public ZipkinSpanWriter(Sampler sampler)
        {
            this.sampler = sampler;
        }

        public Task Write(ISpanStore spanStore, IEnumerable<Span> spans)
        {
            var sampledSpans = spans.Where(s => (s.debug.HasValue && s.debug.Value) || sampler.IsSampled(s.traceId)).ToList();
            return spanStore.Accept(sampledSpans);
        }
    }
}