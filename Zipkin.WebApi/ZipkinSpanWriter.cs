using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Zipkin.Core;

namespace Zipkin.WebApi
{
    public class ZipkinSpanWriter
    {
        public Sampler sampler { get; set; }

        public ZipkinSpanWriter(Sampler sampler)
        {
            this.sampler = sampler;
        }

        public void Write(ISpanStore spanStore, IEnumerable<Span> spans)
        {
            var sampledSpans = spans.Where(s => (s.debug.HasValue && s.debug.Value) || sampler.IsSampled(s.traceId)).ToList();
            spanStore.Accept(sampledSpans);
        }
    }
}