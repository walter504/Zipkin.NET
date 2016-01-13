using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tracing.Core;

namespace Tracing.WebApi
{
    public class SpanWriter
    {
        public Sampler sampler { get; set; }

        public void Write(ISpanStore spanStore, IEnumerable<Span> spans)
        {
            var sampledSpans = spans.Where(s => (s.debug.HasValue && s.debug.Value) || sampler.IsSampled(s.id)).ToList();
            spanStore.Accept(sampledSpans);
        }
    }
}