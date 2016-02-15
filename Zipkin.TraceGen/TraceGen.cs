using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Core;

namespace Zipkin.TraceGen
{
    public class TraceGen
    {
        private static Random rnd = new Random(DateTime.Now.Millisecond);

        private static string[] serviceNames = 
          @"vitae ipsum felis lorem magna dolor porta donec augue tortor auctor
            mattis ligula mollis aenean montes semper magnis rutrum turpis sociis
            lectus mauris congue libero rhoncus dapibus natoque gravida viverra egestas
            lacinia feugiat pulvinar accumsan sagittis ultrices praesent vehicula nascetur
            pharetra maecenas consequat ultricies ridiculus malesuada curabitur convallis
            facilisis hendrerit penatibus imperdiet tincidunt parturient adipiscing
            consectetur pellentesque".Split(' ');

        private static string[] rpcNames =
          @"vivamus fermentum semper porta nunc diam velit adipiscing ut tristique vitae".Split(' ');
        public static string GetRandSvcName()
        {
            return serviceNames[rnd.Next(serviceNames.Length)];
        }

        public static string GetRandRpcName()
        {
            return rpcNames[rnd.Next(rpcNames.Length)];
        }

        private int traces;
        private int maxDepth;

        public TraceGen(int traces, int maxDepth)
        {
            this.traces = traces;
            this.maxDepth = maxDepth;
        }

        public List<Span> Apply()
        {
             
        }

        protected static long GetRandLong()
        {
            return (long)(rnd.NextDouble() * long.MaxValue);
        }

        private class GenTrace
        {
            public List<Span> spans = new List<Span>();
            
            public void AddSpan(string name, long id, long? parentId, long timestamp,
                long duration, List<Annotation> annos, List<BinaryAnnotation> binAnnos)
            {
                spans.Add(new Span(GetRandLong(), name, id, parentId, timestamp, duration, annos, binAnnos));
            }
        }

        private List<string> upstreamServices = new List<string>();
    }
}
