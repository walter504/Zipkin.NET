using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Core;
using Zipkin.Core.Json;

namespace Zipkin.TraceGen
{
    class Program
    {
        static void Main(string[] args)
        {
             
        }

        private void LogEntry(Span span)
        {
            var jsonSpans = new List<JsonSpan>() { new JsonSpan(span) };
            var data = JsonConvert.SerializeObject(jsonSpans);
            using(var client = new WebClient())
            {
                client.BaseAddress = "http://localhost:9411";
                client.UploadString("/api/v1/spans", "POST", data);
            }
        }
    }
}
