using Autofac;
using Autofac.Integration.WebApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Http;
using System.Web.Security;
using System.Web.SessionState;
using Zipkin.Core;
using Zipkin.DbStore;

namespace Zipkin.WebApi
{
    public class Global : System.Web.HttpApplication
    {

        protected void Application_Start(object sender, EventArgs e)
        {
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
            //log4net.Config.XmlConfigurator.Configure();
            builder.Register(c => Sampler.Create(1.0F)).As<Sampler>().SingleInstance();
            builder.RegisterType<ZipkinSpanWriter>();
            builder.RegisterType<DbSpanStore>().As<ISpanStore>();
        }

        protected void Application_BeginRequest(object sender, EventArgs e)
        {

        }

        protected void Application_Error(object sender, EventArgs e)
        {

        }

        protected void Application_End(object sender, EventArgs e)
        {

        }
    }
}