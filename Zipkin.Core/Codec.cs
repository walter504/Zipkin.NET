using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Core.Internal;

namespace Zipkin.Core
{
    public abstract class Codec
    {
        static ThriftCodec THRIFT = new ThriftCodec();

        public static Codec Get(string mediaType)
        {
            if (mediaType.StartsWith("application/x-thrift"))
            {
                return THRIFT;
            }
            return null;
        }

        public abstract Span ReadSpan(byte[] bytes);

        public abstract byte[] WriteSpan(Span value);

        public abstract List<Span> ReadSpans(byte[] bytes);

        public abstract byte[] WriteSpans(List<Span> value);

        public abstract byte[] WriteTraces(List<List<Span>> value);

        public abstract List<DependencyLink> ReadDependencyLinks(byte[] bytes);

        public abstract byte[] WriteDependencyLinks(List<DependencyLink> value);
    }
}
