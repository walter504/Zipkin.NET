using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public enum AnnotationType
    {
        /**
         * Set to 0x01 when {@link BinaryAnnotation#key} is {@link Constants#CLIENT_ADDR} or  {@link
         * Constants#SERVER_ADDR}
         */
        BOOL = 0,
        /** No encoding, or type is unknown. */
        BYTES = 1,
        I16 = 2,
        I32 = 3,
        I64 = 4,
        DOUBLE = 5,
        /** The only type zipkin v1 supports search against. */
        STRING = 6
    }
}
