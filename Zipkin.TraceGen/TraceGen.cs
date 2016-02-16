using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Zipkin.Core;

namespace Zipkin.TraceGen
{
    public class TraceGen
    {
        protected static Random rnd = new Random(DateTime.Now.Millisecond);

        private static string[] serviceNames = Regex.Split(
          @"vitae ipsum felis lorem magna dolor porta donec augue tortor auctor
            mattis ligula mollis aenean montes semper magnis rutrum turpis sociis
            lectus mauris congue libero rhoncus dapibus natoque gravida viverra egestas
            lacinia feugiat pulvinar accumsan sagittis ultrices praesent vehicula nascetur
            pharetra maecenas consequat ultricies ridiculus malesuada curabitur convallis
            facilisis hendrerit penatibus imperdiet tincidunt parturient adipiscing
            consectetur pellentesque", @"\s+");

        private static string[] rpcNames = Regex.Split(
          @"vivamus fermentum semper porta nunc diam velit adipiscing ut tristique vitae", @"\s");
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
            return (new int[this.traces]).SelectMany(i =>
            {
                var start = DateTime.Now.AddHours(-(rnd.Next(8) + 1));
                var trace = new GenTrace();
                //DoRpc(trace, start, rnd.Next(this.maxDepth), GetRandRpcName(), withEndpoint(ep => ep));
                withEndpoint(ep => {
                    DoRpc(trace, start, rnd.Next(this.maxDepth), GetRandRpcName(), ep);
                    return ep;
                });
                return trace.Spans;
            }).ToList();
        }

        protected static long GetRandLong()
        {
            return (long)(rnd.NextDouble() * long.MaxValue);
        }

        private class GenTrace
        {
            public List<Span> Spans = new List<Span>();
            private long traceId = GetRandLong();
            public void AddSpan(string name, long id, long? parentId, long timestamp,
                long duration, List<Annotation> annos, List<BinaryAnnotation> binAnnos)
            {
                Spans.Add(new Span(traceId, name, id, parentId, timestamp, duration, annos, binAnnos));
            }
        }

        private delegate T Act<T>(Endpoint endpoint);

        private List<string> upstreamServices = new List<string>();
        private T withEndpoint<T>(Act<T> f)
        {
            // attempt to get a service name without introducing a loop in the trace DAG
            var svcName = GetRandSvcName();
            var attempts = serviceNames.Length;
            while (attempts > 0 && upstreamServices.Contains(svcName)) 
            {
              svcName = GetRandSvcName();
              attempts--;
            }

            // couldn't find one. create a new one with a random suffix
            if (attempts == 0)
            {
              svcName += (rnd.Next(8000) + 1000);
            }

            upstreamServices.Add(svcName);
            var ret = f(new Endpoint(rnd.Next(), (short)(rnd.Next(8000) + 1000), svcName));
            upstreamServices.Remove(svcName);
            return ret;
        }

        private DateTime DoRpc(
            GenTrace trace, 
            DateTime time, 
            int depth, 
            string spanName,
            Endpoint endpoint, 
            long spanId = long.MinValue, 
            long? parentSpanId = null)
        {
            if (spanId == long.MinValue)
            {
                spanId = GetRandLong();
            }

            var curTime = time.AddMilliseconds(1);

            var svrAnnos = new List<Annotation>();
            svrAnnos.Add(new Annotation(Util.ToUnixTimMicroseconds(curTime), Constants.ServerRecv, endpoint));

            var svrBinAnnos = new List<BinaryAnnotation>();
            var randInt = rnd.Next(3);
            for (var i = 0; i < randInt; i ++)
            {
                svrBinAnnos.Add(new BinaryAnnotation(GetRandSvcName(), Encoding.UTF8.GetBytes(GetRandSvcName()), AnnotationType.STRING, endpoint));
            }

            // simulate some amount of work
            curTime = curTime.AddMilliseconds(rnd.Next(10));

            randInt = rnd.Next(5) + 1;
            for (var i = 0; i < randInt; i ++)
            {
                svrAnnos.Add(new Annotation(Util.ToUnixTimMicroseconds(curTime), GetRandSvcName(), endpoint));
                curTime = curTime.AddMilliseconds(rnd.Next(10));
            }

            if (depth > 0)
            {
                // parallel calls to downstream services
                var times = (new int[rnd.Next(depth) + 1]).Select(i =>
                {
                    return withEndpoint(nextEp =>
                    {
                        var thisSpanId = GetRandLong();
                        var thisParentId = spanId;
                        var rpcName = GetRandRpcName();
                        var annos = new List<Annotation>();
                        var binAnnos = new List<BinaryAnnotation>();

                        var delay = rnd.Next(10) > 6 ? rnd.Next(10) : 0;
                        annos.Add(new Annotation(Util.ToUnixTimMicroseconds(curTime) + delay, Constants.ClientSend, nextEp));
                        var thisTime = DoRpc(trace, curTime, rnd.Next(depth), rpcName, nextEp, thisSpanId, thisParentId).AddMilliseconds(1);
                        annos.Add(new Annotation(Util.ToUnixTimMicroseconds(thisTime), Constants.ClientRecv, nextEp));

                        var thisTimestamp = annos[0].timestamp;
                        var thisDuration = annos[1].timestamp - annos[0].timestamp;
                        trace.AddSpan(rpcName, thisSpanId, thisParentId, thisTimestamp, thisDuration, annos, binAnnos);
                        return thisTime;
                    });
                });
                curTime = times.Max();
            }

            svrAnnos.Add(new Annotation(Util.ToUnixTimMicroseconds(curTime), Constants.ServerSend, endpoint));
            var timestamp = svrAnnos[0].timestamp;
            var duration = svrAnnos[svrAnnos.Count - 1].timestamp - svrAnnos[0].timestamp;
            trace.AddSpan(spanName, spanId, parentSpanId, timestamp, duration, svrAnnos, svrBinAnnos);
            return curTime;
        }
    }
}
