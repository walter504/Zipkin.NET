using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.DbStore.Models
{
    public class zipkin_spans
    {
        public long trace_id { get; set; }
        public long id { get; set; }
        public string name { get; set; }
        public long? parent_id { get; set; }
        public bool? debug { get; set; }
        public long? start_ts { get; set; }
        public long? duration { get; set; }

    }
}
