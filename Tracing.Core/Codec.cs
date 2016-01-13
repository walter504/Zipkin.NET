using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public abstract class Codec
    {
        public abstract Codec Get(string mediaType);

        public abstract Span ReadSpan(byte[] bytes);

        public abstract byte[] WriteSpan(Span value);

        public abstract List<Span> ReadSpans(byte[] bytes);

        public abstract byte[] WriteSpans(List<Span> value);

        public abstract byte[] WriteTraces(List<List<Span>> value);

        public abstract List<DependencyLink> ReadDependencyLinks(byte[] bytes);

        public abstract byte[] WriteDependencyLinks(List<DependencyLink> value);
    }
}
