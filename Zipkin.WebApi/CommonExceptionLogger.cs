using System;
using System.Web.Http.ExceptionHandling;
using Common.Logging;

namespace Zipkin.WebApi
{
    public class CommonExceptionLogger : ExceptionLogger
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(CommonExceptionLogger));
        public override void Log(ExceptionLoggerContext context)
        {
            log.Error("unexpected exception", context.ExceptionContext.Exception);
        }
    }
}