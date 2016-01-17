using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core.Json
{
    public class JsonAnnotation
    {
        public long timestamp { get; set; }
        public string value { get; set; }
        public JsonEndpoint endpoint { get; set; }

        public JsonAnnotation()
        {
        }

        public JsonAnnotation(Annotation annotation)
        {
            this.timestamp = annotation.timestamp;
            this.value = annotation.value;
            this.endpoint = new JsonEndpoint(annotation.endpoint);
        }

        public Annotation Invert()
        {
            return new Annotation(timestamp, value, endpoint.Invert());
        }
    }
}
