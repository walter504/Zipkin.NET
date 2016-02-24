using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;

namespace Zipkin.UI.Web.Helpers
{
    public static class WebAppSettings
    {
        public static string QueryHost
        {
            get
            {
                return ConfigurationManager.AppSettings["QueryHost"];
            }
        }
    }
}