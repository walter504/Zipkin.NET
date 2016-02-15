using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using Zipkin.Core;

namespace Zipkin.UI.Web.Helpers
{
    public class Util
    {
        /// <summary>
        /// format durtion
        /// </summary>
        /// <param name="duration">μs</param>
        /// <returns></returns>
        public static string FormatDurtion(long duration)
        {
            var sb = new StringBuilder();
            var ts = TimeSpan.FromTicks(duration * 10);
            if (duration != 0)
            {
                if (ts.Days >= 1)
                {
                    sb.AppendFormat(" {0}{1}", ts.Days, ts.Days == 1 ? "day" : "days");
                }
                if (ts.Hours >= 1)
                {
                    sb.AppendFormat(" {0}{1}", ts.Hours, ts.Hours == 1 ? "hr" : "hrs");
                }
                if (ts.Minutes >= 1)
                {
                    sb.AppendFormat(" {0}min", ts.Minutes);
                }
                var newts = ts - new TimeSpan(ts.Days, ts.Hours, ts.Minutes, 0);
                if (ts.Seconds >= 1)
                {
                    sb.AppendFormat(" {0:#.000}s", newts.Ticks / 10000000.0);
                }
                else if (ts.Milliseconds >= 1)
                {
                    sb.AppendFormat(" {0:#.000}ms", newts.Ticks / 10000.0);
                }
                else if (newts.Ticks / 10 >= 1)
                {
                    sb.AppendFormat(" {0}μs", newts.Ticks / 10);
                }
            }
            return sb.Length == 0 ? string.Empty : sb.Remove(0, 1).ToString();
        }

        public static string GetAnnotationName(string value)
        {
            string name;
            if (!Constants.CoreAnnotationNames.TryGetValue(value, out name))
            {
                name = value;
            }
            return name;
        }

        public static IEnumerable<Span> GetRootSpans(IEnumerable<Span> spans)
        {
            var idSpan = GetIdToSpanMap(spans);
            return spans.Where(s => !s.parentId.HasValue || !idSpan.ContainsKey(s.parentId.Value)).ToList();
        }

        public static Span GetRootMostSpan(IEnumerable<Span> spans)
        {
            var root = spans.FirstOrDefault(s => !s.parentId.HasValue);
            if (root == null && spans.Count() != 0)
            {
                var idSpan = GetIdToSpanMap(spans);
                root = RecursiveGetRootMostSpan(idSpan, spans.First());
            }
            return root;
        }
        public static Span RecursiveGetRootMostSpan(Dictionary<long, Span> idSpan, Span prevSpan)
        {
            Span span = null;
            if (prevSpan.parentId.HasValue && idSpan.TryGetValue(prevSpan.parentId.Value, out span))
            {
                return RecursiveGetRootMostSpan(idSpan, span);
            }
            return prevSpan;
        }

        public static Dictionary<long, Span> GetIdToSpanMap(IEnumerable<Span> spans)
        {
            return spans.ToDictionary(s => s.id, s => s);
        }
    }
}