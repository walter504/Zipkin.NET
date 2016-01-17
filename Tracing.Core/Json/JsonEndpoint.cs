using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core.Json
{
    public class JsonEndpoint
    {
        public string serviceName { get; set; }
        public string ipv4 { get; set; }
        public int? port { get; set; }

        public JsonEndpoint()
        {
        }

        public JsonEndpoint(Endpoint host)
        {
            this.serviceName = host.serviceName;
            this.ipv4 = host.GetHostAddress();
            this.port = host.GetUnsignedPort();
        }

        public Endpoint Invert()
        {
            return new Endpoint(InetAddresses.IP4ToInt(this.ipv4), (short)(port ?? 0), serviceName.ToLower());
        }
    }
}
