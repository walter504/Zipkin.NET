using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class Annotation : IComparable<Annotation>
    {
        public long timestamp { get; set; }
        public string value { get; set; }
        public Endpoint endpoint { get; set; }

        public string serviceName
        {
            get
            {
                return endpoint == null ? string.Empty : endpoint.serviceName;
            }
        }

        public Annotation(long timestamp, string value, Endpoint endpoint)
        {
            this.timestamp = timestamp;
            this.value = Ensure.ArgumentNotNull(value, "value");
            this.endpoint = endpoint;
        }

        //public String toString() {
        //    return JsonCodec.ANNOTATION_ADAPTER.toJson(this);
        //}

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is Annotation)
            {
                Annotation that = (Annotation)o;
                return (this.timestamp == that.timestamp)
                    && (this.value.Equals(that.value))
                    && Util.Equal(this.endpoint, that.endpoint);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= (int)((timestamp >> 32) ^ timestamp);
            h *= 1000003;
            h ^= value.GetHashCode();
            h *= 1000003;
            h ^= (endpoint == null) ? 0 : endpoint.GetHashCode();
            return h;
        }

        public int CompareTo(Annotation that)
        {
            if (this == that) return 0;
            int byTimestamp = timestamp.CompareTo(that.timestamp);
            if (byTimestamp != 0) return byTimestamp;
            return value.CompareTo(that.value);
        }
    }
}
