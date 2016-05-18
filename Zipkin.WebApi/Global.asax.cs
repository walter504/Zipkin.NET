using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Http;
using System.Web.Security;
using System.Web.SessionState;
using Autofac;
using Autofac.Integration.WebApi;
using Common.Logging;
using Microsoft.Extensions.Configuration;
using Zipkin;
using Zipkin.Storage;
using Zipkin.Storage.Cassandra;
using Zipkin.Storage.MySql;

namespace Zipkin.WebApi
{
    public class Global : System.Web.HttpApplication
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Global));

        protected void Application_Start(object sender, EventArgs e)
        {
            log.Info("WebApi Start");

            GlobalConfiguration.Configure(WebApiConfig.Register);

            var config = GlobalConfiguration.Configuration;
            var builder = new ContainerBuilder();

            /* Web API */
            // Register your Web API controllers.
            builder.RegisterApiControllers(Assembly.GetExecutingAssembly());
            // OPTIONAL: Register the Autofac filter provider.
            builder.RegisterWebApiFilterProvider(config);

            // 注册其它模块
            Register(builder);

            // container 必须在注册完所有模块后创建
            // Perform registrations and build the container.
            var container = builder.Build();
            // Set the Web API dependency resolver to be Autofac.
            config.DependencyResolver = new AutofacWebApiDependencyResolver(container);
        }

        private static void Register(ContainerBuilder builder)
        {
            builder.Register(c => Sampler.Create(1.0F)).As<Sampler>().SingleInstance();
            builder.RegisterType<ZipkinSpanWriter>().SingleInstance();

            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddJsonFile("configs/cassandra.json");
            var cofiguration = configurationBuilder.Build();
            var props = cofiguration.GetZipkinCassandraProperties();
            var storage = props.ToBuilder().Build();
            builder.Register(c => storage.SpanStore).As<ISpanStore>().SingleInstance();
            builder.Register(c => storage.DependencyStore).As<IDependencyStore>().SingleInstance();
        }

        protected void Application_BeginRequest(object sender, EventArgs e)
        {
        }

        protected void Application_Error(object sender, EventArgs e)
        {
            HttpException exception = Server.GetLastError() as HttpException;
            if (exception is HttpException)
            {
                if (exception.GetHttpCode() != 404)
                {
                    log.ErrorFormat("http error(HttpCode-{0}): {1}", exception.GetHttpCode(), exception);
                }
            }
            else
            {
                log.Error("unexpected exception", exception);
            }
        }

        protected void Application_End(object sender, EventArgs e)
        {
            log.Info("WebApi End");
        }
    }
}