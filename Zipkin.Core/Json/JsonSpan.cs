using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core.Json
{
    public class JsonSpan
    {
        public string traceId { get; set; }
        public string name { get; set; }
        public string id { get; set; }
        public string parentId { get; set; }
        public long? timestamp { get; set; }
        public long? duration { get; set; }
        public List<JsonAnnotation> annotations { get; set; }
        public List<JsonBinaryAnnotation> binaryAnnotations { get; set; }
        public bool? debug { get; set; }

        public JsonSpan()
        {
        }

        public JsonSpan(Span span)
        {
            traceId = Util.LongToHex(span.traceId);
            name = span.name;
            id = Util.LongToHex(span.id);
            parentId = span.parentId.HasValue ? Util.LongToHex(span.parentId.Value) : null;
            timestamp = span.timestamp;
            duration = span.duration;
            if (null != span.annotations)
            {
                annotations = span.annotations.Select(a => new JsonAnnotation(a)).ToList();
            }
            if (null != span.binaryAnnotations)
            {
                binaryAnnotations = span.binaryAnnotations.Select(a => new JsonBinaryAnnotation(a)).ToList();
            }
            debug = span.debug;
        }

        public Span Invert()
        {
            return new Span(Util.HexToLong(traceId),
                name,
                Util.HexToLong(id),
                string.IsNullOrEmpty(parentId) ? null : new Nullable<long>(Util.HexToLong(parentId)),
                timestamp,
                duration,
                annotations == null ? new List<Annotation>() : annotations.Select(a => a.Invert()).ToList(),
                annotations == null ? new List<BinaryAnnotation>() : binaryAnnotations.Select(a => a.Invert()).ToList(),
                debug);
        }
    }
}
