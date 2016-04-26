using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Zipkin.Internal
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

        protected static DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
        
        public static long CurrentTimeSeconds()
        {
            return ToUnixTimeSeconds(DateTime.Now);
        }

        public static long CurrentTimeMilliseconds()
        {
            return ToUnixTimeMilliseconds(DateTime.Now);
        }

        public static long ToUnixTimeSeconds(DateTime dt)
        {
            TimeSpan timespan = TimeZoneInfo.ConvertTimeToUtc(dt).Subtract(UnixEpoch);
            return timespan.Ticks / 10000000;
        }

        public static long ToUnixTimeMilliseconds(DateTime dt)
        {
            TimeSpan timespan = TimeZoneInfo.ConvertTimeToUtc(dt).Subtract(UnixEpoch);
            return timespan.Ticks / 10000;
        }

        public static long ToUnixTimMicroseconds(DateTime dt)
        {
            TimeSpan timespan = TimeZoneInfo.ConvertTimeToUtc(dt).Subtract(UnixEpoch);
            return timespan.Ticks / 10;
        }

        public static DateTime FromUnixTimeSeconds(long seconds)
        {
            return UnixEpoch.AddSeconds(seconds).ToLocalTime();
        }

        public static DateTime FromUnixTimeMilliseconds(long mills)
        {
            return UnixEpoch.AddMilliseconds(mills).ToLocalTime();
        }

        public static DateTime FromUnixTimeMicroseconds(long micros)
        {
            return UnixEpoch.AddTicks(micros * 10).ToLocalTime();
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
