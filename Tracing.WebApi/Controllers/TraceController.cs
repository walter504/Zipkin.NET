using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Tracing.WebApi.Controllers
{
    [Route("api/v1")]
    public class TraceController : ApiController
    {
        // GET api/trace
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/trace/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/trace
        public void Post([FromBody]string value)
        {
        }
    }
}
