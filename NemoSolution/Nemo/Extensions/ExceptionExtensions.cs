using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Nemo.Extensions
{
    public static class ExceptionExtensions
    {
        private const string ExceptionPrepForRemotingMethodName = "PrepForRemoting";
        private static readonly MethodInfo prepForRemoting = typeof(Exception).GetMethod("PrepForRemoting", BindingFlags.NonPublic | BindingFlags.Instance);

        public static void ThrowIfNull<T>(this T value, string name)
            where T : class
        {
            if (value == null)
            {
                throw new ArgumentNullException(name);
            }
        }

        public static void ThrowIfNegative(this int value, string name)
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(name);
            }
        }

        public static void ThrowIfNonPositive(this int value, string name)
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(name);
            }
        }

        public static bool IsCritical(this Exception exception)
        {
            return exception is AccessViolationException || exception is NullReferenceException || exception is StackOverflowException || exception is OutOfMemoryException || exception is ThreadAbortException;
        }

        public static Exception PrepareForRethrow(this Exception exception)
        {
            exception.ThrowIfNull("exception");
            prepForRemoting.Invoke(exception, new object[0]);
            return exception;
        }

        public static void Rethrow(this Exception exception)
        {
            throw exception.PrepareForRethrow();
        }

        public static IEnumerable<Exception> Traverse(this Exception exception)
        {
            yield return exception;
            if (exception.InnerException != null)
            {
                foreach (var innerException in Traverse(exception.InnerException))
                {
                    yield return innerException;
                }
            }
        }

        public static string Format(this Exception exception, string delimiter = "\r\n")
        {
            return exception.Traverse().Select(e => e.Message).ToDelimitedString(delimiter);
        }
    }
}
