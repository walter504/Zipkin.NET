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

        protected static readonly DateTime unixTPStart =
            TimeZone.CurrentTimeZone.ToUniversalTime(new DateTime(1970, 1, 1));

        public static long CurrentTimeSeconds()
        {
            return ToUnixTimeSeconds(DateTime.UtcNow);
        }

        public static long CurrentTimeMilliSeconds()
        {
            var toNow = DateTime.UtcNow - unixTPStart;
            return (long)Math.Round(toNow.TotalMilliseconds);
        }

        public static long ToUnixTimeSeconds(DateTime dt)
        {
            TimeSpan toNow = dt.ToUniversalTime().Subtract(unixTPStart);
            return (long)Math.Round(toNow.TotalSeconds);
        }

        public static long ToUnixTimeMilliseconds(DateTime dt)
        {
            TimeSpan toNow = dt.ToUniversalTime().Subtract(unixTPStart);
            return (long)Math.Round(toNow.TotalMilliseconds);
        }

        public static DateTime FromUnixTimeSeconds(long seconds)
        {
            return unixTPStart.AddSeconds(seconds);
        }

        public static DateTime FromUnixTimeMilliseconds(long seconds)
        {
            return unixTPStart.AddMilliseconds(seconds);
        }


        public static long HexToLong(string hex)
        {
            return long.Parse(hex, System.Globalization.NumberStyles.HexNumber);
        }

        public static string LongToHex(long input)
        {
            return input.ToString("x4");
        }
    }
}
