using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Zipkin.UI.Web.ViewModels
{
    public class MustacheServiceDuration
    {
        public string name { get; set; }
        public int count { get; set; }
        public long max { get; set; }
    }
}