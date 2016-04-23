using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class BinaryAnnotation
    {
        /**
         * Name used to lookup spans, such as "http.uri" or "finagle.version".
         */
        public string key { get; set; }
        public byte[] value { get; set; }
        public AnnotationType type { get; set; }
        public Endpoint endpoint { get; set; }

        public string serviceName
        {
            get
            {
                return endpoint == null ? string.Empty : endpoint.serviceName;
            }
        }

        public BinaryAnnotation(string key, Endpoint endpoint)
            : this(key, new byte[] { 1 }, AnnotationType.BOOL, Ensure.ArgumentNotNull(endpoint, "endpoint"))
        {
        }

        /** string values are the only queryable type of binary annotation. */
        public BinaryAnnotation(string key, string value, Endpoint endpoint)
            : this(key, Encoding.UTF8.GetBytes(value), AnnotationType.STRING, endpoint)
        {
        }

        public BinaryAnnotation(string key, byte[] value, AnnotationType type, Endpoint endpoint)
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
    }
}
