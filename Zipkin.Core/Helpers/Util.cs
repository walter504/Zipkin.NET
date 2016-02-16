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

        public static List<T> SortedList<T>(IList<T> input) where T : IComparable<T>
        {
            if (input == null || 0 == input.Count) return new List<T>();
            if (input.Count == 1) return new ReadOnlyCollection<T>(input).ToList();
            List<T> result = input.ToList();
            result.Sort();
            return new ReadOnlyCollection<T>(result).ToList();
        }

        protected static readonly DateTime unixTPStart =
            TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));

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
            TimeSpan toNow = dt.Subtract(unixTPStart);
            return (long)Math.Round(toNow.TotalSeconds);
        }

        public static long ToUnixTimeMilliseconds(DateTime dt)
        {
            TimeSpan toNow = dt.Subtract(unixTPStart);
            return (long)Math.Round(toNow.TotalMilliseconds);
        }

        public static long ToUnixTimMicroseconds(DateTime dt)
        {
            TimeSpan toNow = dt.Subtract(unixTPStart);
            return toNow.Ticks / 10;
        }

        public static DateTime FromUnixTimeSeconds(long seconds)
        {
            return unixTPStart.AddSeconds(seconds);
        }

        public static DateTime FromUnixTimeMilliseconds(long mills)
        {
            return unixTPStart.AddMilliseconds(mills);
        }

        public static DateTime FromUnixTimeMicros(long micros)
        {
            return unixTPStart.AddTicks(micros * 10);
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
