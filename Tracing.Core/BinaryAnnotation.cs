using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public class BinaryAnnotation
    {
        /**
         * Name used to lookup spans, such as "http.uri" or "finagle.version".
         */
        public string key;
        public byte[] value;
        public Type type;
        public Endpoint endpoint;

        public BinaryAnnotation(string key, Endpoint endpoint)
            : this(key, new byte[] { 1 }, Type.BOOL, Ensure.ArgumentNotNull(endpoint, "endpoint"))
        {
        }

        /** string values are the only queryable type of binary annotation. */
        public BinaryAnnotation(string key, string value, Endpoint endpoint)
            : this(key, Encoding.UTF8.GetBytes(value), Type.STRING, endpoint)
        {
        }

        public BinaryAnnotation(string key, byte[] value, Type type, Endpoint endpoint)
        {
            this.key = Ensure.ArgumentNotNull(key, "key");
            this.value = Ensure.ArgumentNotNull(value, "value");
            this.type = Ensure.ArgumentNotNull(type, "type");
            this.endpoint = endpoint;
        }

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is BinaryAnnotation)
            {
                BinaryAnnotation that = (BinaryAnnotation)o;
                return (this.key.Equals(that.key))
                    && (Array.Equals(this.value, that.value))
                    && (this.type.Equals(that.type))
                    && Util.Equal(this.endpoint, that.endpoint);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= key.GetHashCode();
            h *= 1000003;
            h ^= value.GetHashCode();
            h *= 1000003;
            h ^= type.GetHashCode();
            h *= 1000003;
            h ^= (endpoint == null) ? 0 : endpoint.GetHashCode();
            return h;
        }

        public enum Type
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
}
