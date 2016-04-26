using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.WebApi.Controllers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Zipkin;
using Newtonsoft.Json;

namespace Zipkin.WebApi.Controllers.Tests
{
    [TestClass()]
    public class TraceControllerTests
    {
        [TestMethod()]
        public void GetServiceNamesTest()
        {
            Assert.Fail();
        }

        [TestMethod()]
        public void PostSpansTest()
        {
            var ep1 = new Endpoint(123, 123, "service1");
            var ep2 = new Endpoint(234, 234, "service2");
            var ep3 = new Endpoint(345, 345, "service3");
            var ep4 = new Endpoint(456, 456, "service4");

            var ann1 = new Annotation(100000, Constants.ClientSend, ep1);
            var ann2 = new Annotation(150000, Constants.ClientRecv, ep1);
            var spans1 = new List<Span>() {
                new Span(1, "methodcall", 666, 2, annotations: new List<Annotation>() {ann1, ann2})
            };

            var ann3 = new Annotation(101000, Constants.ClientSend, ep2);
            var ann4 = new Annotation(501000, Constants.ClientRecv, ep2);
            var spans2 = new List<Span>() {
                new Span(2, "methodcall", 2, annotations: new List<Annotation>() {ann3, ann4})
            };

            var ann5 = new Annotation(99000, Constants.ClientSend, ep2);
            var ann6 = new Annotation(199000, Constants.ClientRecv, ep2);
            var spans3 = new List<Span>() {
                new Span(3, "methodcall", 3, annotations: new List<Annotation>() {ann5, ann6})
            };

            // get some server action going on
            var ann7 = new Annotation(110000, Constants.ServerRecv, ep2);
            var ann8 = new Annotation(140000, Constants.ServerSend, ep2);
            var spans4 = new List<Span>() {
                new Span(2, "methodcall", 666, 2, annotations: new List<Annotation>() {ann1, ann2}),
                new Span(2, "methodcall", 666, 2, annotations: new List<Annotation>() {ann7, ann8})
            };

            var ann9 = new Annotation(60000, Constants.ClientSend, ep3);
            var ann10 = new Annotation(65000, "annotation", ep3);
            var ann11 = new Annotation(100000, Constants.ClientRecv, ep3);
            var bAnn1 = new BinaryAnnotation("annotation", Encoding.UTF8.GetBytes("ann"), AnnotationType.STRING, ep3);
            var bAnn2 = new BinaryAnnotation("binary", Encoding.UTF8.GetBytes("ann"), AnnotationType.BYTES, ep3);
            var spans5 = new List<Span>() {
                new Span(5, "methodcall", 666, 2, annotations: new List<Annotation>() {ann9, ann10, ann11}, binaryAnnotations: new List<BinaryAnnotation>() {bAnn1, bAnn2})
            };

            var ann13 = new Annotation(100000, Constants.ClientSend, ep4);
            var ann14 = new Annotation(150000, Constants.ClientRecv, ep4);
            var spans6 = new List<Span>() {
                new Span(6, "methodcall", 669, 2, annotations: new List<Annotation>() {ann13, ann14})
            };

            var allSpans = spans1.Concat(spans2).Concat(spans3).Concat(spans4).Concat(spans5).Concat(spans6);
            string postBody = JsonConvert.SerializeObject(allSpans);
            string contentType = "application/json; charset=utf-8";
            Assert.Fail();
        }
    }
}
