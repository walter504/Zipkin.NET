using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Zipkin.UI.Web.ViewModels
{
    public class MustacheTraceSummary
    {
        public string traceId { get; set; }
        public string startTs { get; set; }
        public long timestamp { get; set; }
        public long duration { get; set; }
        public string durationStr { get; set; }
        public int servicePercentage { get; set; }
        public int spanCount { get; set; }
        public IEnumerable<MustacheServiceDuration> serviceDurations { get; set; }
        public int width { get; set; }
    }
}