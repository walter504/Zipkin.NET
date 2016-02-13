using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class CorrectForClockSkew
    {
        class ClockSkew
        {
            Endpoint endpoint { get; set; }
            long skew { get; set; }
            public ClockSkew(Endpoint endpoint, long skew)
            {
                this.endpoint = endpoint;
                this.skew = skew;
            }
        }
    }
}
