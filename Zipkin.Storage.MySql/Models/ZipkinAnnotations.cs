using System;

namespace Zipkin.Storage.MySql.Models
{
    public class zipkin_annotations
    {
        public long trace_id { get; set; }
        public long span_id { get; set; }
        public string a_key { get; set; }
        public byte[] a_value { get; set; }
        public int a_type { get; set; }
        public long? a_timestamp { get; set; }
        public int? endpoint_ipv4 { get; set; }
        public short? endpoint_port { get; set; }
        public string  endpoint_service_name { get; set; }
    }
}
