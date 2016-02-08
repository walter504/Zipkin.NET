using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class Util
    {
        public static bool Equal(Object a, Object b)
        {
            return a == b || (a != null && a.Equals(b));
        }

        public static IList<T> SortedList<T>(IList<T> input) where T : IComparable<T>
        {
            if (input == null || 0 == input.Count) return new Collection<T>();
            if (input.Count == 1) return new ReadOnlyCollection<T>(input);
            List<T> result = input.ToList();
            result.Sort();
            return new ReadOnlyCollection<T>(result);
        }

        public static long GetCurrentTimeStamp()
        {
            var t = DateTime.UtcNow - new DateTime(1970, 1, 1);
            return Convert.ToInt64(t.TotalMilliseconds * 1000);
        }
    }
}
