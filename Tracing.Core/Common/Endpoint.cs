using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public class Endpoint
    {
        public int ipv4 { get; set; }
        public short port { get; set; }
        public string serviceName { get; set; }

        public Endpoint()
        {
        }

        public Endpoint(int ipv4, short port, string serviceName)
        {
            Ensure.ArgumentAssert(serviceName.ToLower() == serviceName, "serviceName must be lowercase");
            this.ipv4 = ipv4;
            this.port = port;
            this.serviceName = serviceName.ToLower();
        }

        public string GetHostAddress()
        {
            return string.Format("{0}.{1}.{2}.{3}", 
                (ipv4 >> 24) & 0xFF,
                (ipv4 >> 16) & 0xFF,
                (ipv4 >> 8) & 0xFF,
                ipv4 & 0xFF);
        }

        public int GetUnsignedPort()
        {
            return port & 0xFFFF;
        }

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is Endpoint)
            {
                Endpoint that = (Endpoint)o;
                return (this.ipv4 == that.ipv4)
                    && (this.port == that.port)
                    && this.serviceName.Equals(that.serviceName);
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
            h ^= port;
            return h;
        }
    }
}
