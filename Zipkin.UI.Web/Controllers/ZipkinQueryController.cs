using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Zipkin.UI.Web.Helpers;

namespace Zipkin.UI.Web.Controllers
{
    [RoutePrefix("api/v1")]
    public class ZipkinQueryController : Controller
    {
        [Route("dependencies")]
        [Route("services")]
        [Route("spans")]
        [Route("trace/{id}")]
        [Route("traces")]
        public ContentResult Route()
        {
            var baseUri = Request.Url.AbsolutePath;
            var client = new RestClient(WebAppSettings.QueryHost);
            var request = new RestRequest(baseUri);
            foreach (var key in Request.QueryString.AllKeys)
            {
                request.AddParameter(key, Request.QueryString[key]);
            }
            var response = client.Execute(request);
            foreach(var param in response.Headers)
            {
                Response.AddHeader(param.Name, param.Value.ToString());
            }
            Response.StatusCode = (int)response.StatusCode;
            return Content(response.Content, response.ContentType);
        }
    }
}