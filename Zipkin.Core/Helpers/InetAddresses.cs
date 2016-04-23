using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class InetAddresses
    {
        public static int IP4ToInt(string ipv4string)
        {
            int ipv4 = 0;
            foreach(var b in ipv4string.Split('.'))
            {
                ipv4 = ipv4 << 8 | (Int32.Parse(b) & 0xff);
            }
            return ipv4;
        }
    }
}
