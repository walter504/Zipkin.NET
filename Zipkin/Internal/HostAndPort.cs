using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Internal
{
    public class HostAndPort
    {
        private string host;
        private int? port;

        public string HostText
        {
            get
            {
                return host;
            }
        }

        public long HostLong
        {
            get
            {
                return InetAddresses.IP4ToInt(host);
            }
        }

        public int Port
        {
            get
            {
                return port ?? 0;
            }
        }

        private HostAndPort()
        {
        }

        public int GetPortOrDefault(int defaultPort)
        {
            return port ?? defaultPort;
        }

        public static HostAndPort FromString(string dest)
        {
            var segs = dest.Split(':');
            var hostAndPort = new HostAndPort();
            hostAndPort.host = segs[0];
            if (segs.Length >= 2)
            {
                hostAndPort.port = int.Parse(segs[0]);
            }
            return hostAndPort;
        }
    }
}
