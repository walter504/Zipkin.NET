using System;
using System.Diagnostics.CodeAnalysis;

namespace Zipkin.Internal
{
    public static class Ensure
    {
        public static T ArgumentNotNull<T>(T reference, string errorMessageTemplate, params object[] errorMessageArgs) {
            if (reference == null) {
              // If either of these parameters is null, the right thing happens anyway
                throw new ArgumentNullException(string.Format(errorMessageTemplate, errorMessageArgs));
            }
            return reference;
        }

        /// <summary>
        /// Checks an argument to ensure it isn't null.
        /// </summary>
        /// <param name = "value">The argument value to check</param>
        /// <param name = "name">The name of the argument</param>
        public static void ArgumentNotNull(object value, string name)
        {
            if (value != null) return;
            throw new ArgumentNullException(name);
        }

        /// <summary>
        /// Checks a string argument to ensure it isn't null or empty.
        /// </summary>
        /// <param name = "value">The argument value to check</param>
        /// <param name = "name">The name of the argument</param>
        public static void ArgumentNotNullOrEmptystring(string value, string name)
        {
            ArgumentNotNull(value, name);
            if (!string.IsNullOrWhiteSpace(value)) return;
            throw new ArgumentException("string cannot be empty", name);
        }

        public static void ArgumentAssert(bool expression, string errorMessageTemplate, params object[] errorMessageArgs)
        {
            if (!expression) 
            {
                throw new ArgumentException(string.Format(errorMessageTemplate, errorMessageArgs));
            }
        }

        /// <summary>
        /// Checks a timespan argument to ensure it is a positive value.
        /// </summary>
        /// <param name = "value">The argument value to check</param>
        /// <param name = "name">The name of the argument</param>
        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        public static void GreaterThanZero(TimeSpan value, string name)
        {
            ArgumentNotNull(value, name);

            if (value.TotalMilliseconds > 0) return;

            throw new ArgumentException("Timespan must be greater than zero", name);
        }
    }
}
