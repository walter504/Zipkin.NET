using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public class Endpoint
    {
        public String serviceName;
        public int ipv4;
        public short? port;

        public Endpoint()
        {
        }

        Endpoint(string serviceName, int ipv4, short? port)
        {
            this.serviceName = serviceName.ToLower();
            this.ipv4 = ipv4;
            this.port = port;
        }
        Endpoint(String serviceName, int ipv4)
            : this(serviceName, ipv4, null)
        {
        }

        Endpoint(string serviceName, int ipv4, int port)
        {
            this.serviceName = serviceName;
            this.ipv4 = ipv4;
            this.port = (short)(port & 0xffff);
        }

        //public override string toString() {
        //  return JsonCodec.ENDPOINT_ADAPTER.toJson(this);
        //}

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is Endpoint)
            {
                Endpoint that = (Endpoint)o;
                return (this.serviceName.Equals(that.serviceName))
                    && (this.ipv4 == that.ipv4)
                    && Util.Equal(this.port, that.port);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= serviceName.GetHashCode();
            h *= 1000003;
            h ^= ipv4;
            h *= 1000003;
            h ^= (port == null) ? 0 : port.GetHashCode();
            return h;
        }
    }
}
