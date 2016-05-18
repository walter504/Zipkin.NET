﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Json;

namespace Zipkin.TraceGen
{
    class Program
    {
        private static int traceCount = 20;
        private static int maxDepth = 7;

        private static string queryDest = "http://localhost:9411";
        private static bool generateOnly = false;

        private static WebClient queryClient = new WebClient()
        {
            BaseAddress = queryDest 
        };

        static void Main(string[] args)
        {
            GenerateTraces();
            Console.ReadKey();
        }

        static void GenerateTraces()
        {
            //var traceGen = new TraceGen(traceCount, maxDepth);
            //var traces = traceGen.Apply();
            //var jsonSpans = traces.Select(s => new JsonSpan(s)).ToList();
            //var data = JsonConvert.SerializeObject(jsonSpans);

            //var data = "";
            //var jsonSpans = JsonConvert.DeserializeObject<List<JsonSpan>>(data);
            //var traces = jsonSpans.Select(js => js.Invert()).ToList();

            Parallel.For(1, 100000, new ParallelOptions() { MaxDegreeOfParallelism = 6 }, (i) => 
            {
                var startTs = DateTime.Now.Ticks;
                var traceGen = new TraceGen(traceCount, maxDepth);
                var traces = traceGen.Apply();
                LogEntry(traces);
                Console.WriteLine("Num:{0} --> post {1} spans cost {2}ms", i, traces.Count, (DateTime.Now.Ticks - startTs) / 10000);
            });

            if (!generateOnly)
            {
            }
        }

        static void LogEntry(List<Span> spans)
        {
            var thriftCodec = new ThriftCodec();
            var bytes = thriftCodec.WriteSpans(spans);
            WebClient queryClient = new WebClient()
            {
                BaseAddress = queryDest
            };
            queryClient.Encoding = System.Text.Encoding.UTF8;
            queryClient.Headers.Add("Content-Type", "application/x-thrift");
            queryClient.UploadData("/api/v1/spans", "POST", bytes);
        }

        static void querySpan(
            string service,
            string span,
            string annotation,
            string key,
            string value,
            int limit)
        {
            Console.WriteLine("Querying for service name: {0} and span name: {1}", service, span);
            var traces = GetTraces(string.Format("/api/v1/traces?serviceName={0}&spanName={1}&limit={2}", service, span, limit));
            PrintTraces(traces);

            Console.WriteLine("Querying for service name: {0}", service);
            traces = GetTraces(string.Format("/api/v1/traces?serviceName={0}&limit={1}", service, limit));
            PrintTraces(traces);

            Console.WriteLine("Querying for service name: {0} and annotation: {1}", service, annotation);
            traces = GetTraces(string.Format("/api/v1/traces?serviceName={0}&annotationQuery={1}&limit={2}", service, annotation, limit));
            PrintTraces(traces);

            Console.WriteLine("Querying for service name: {0} and kv annotation: {1} -> {2}", service, key, value);
            traces = GetTraces(string.Format("/api/v1/traces?serviceName={0}&annotationQuery={1}={2}&limit={3}", service, key, value, limit));
            PrintTraces(traces);
        }

        static List<List<Span>> GetTraces(string uri)
        {
            using(Stream stream = queryClient.OpenRead(uri))
            using (StreamReader sr = new StreamReader(stream))
            {
                string json = sr.ReadToEnd();
                var traces = JsonConvert.DeserializeObject<List<List<JsonSpan>>>(json);
                return traces.Select(t => t.Select(js => js.Invert()).ToList()).ToList();
            }
        }

        static void PrintTraces(List<List<Span>> traces)
        {
            foreach (var trace in traces)
            {
                foreach (var span in trace)
                {
                    Console.WriteLine("Got span: ", span);
                }
            }
        }
    }
}
