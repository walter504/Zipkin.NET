﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Zipkin.UI.Web.Controllers
{
    public class TracesController : Controller
    {
        // GET: Traces
        public ActionResult Index(string id)
        {
            return View();
        }
    }
}