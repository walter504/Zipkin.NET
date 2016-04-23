using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Zipkin.UI.Web.Controllers
{
    public class DependencyController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }
    }
}