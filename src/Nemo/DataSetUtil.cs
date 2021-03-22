using Nemo.Utilities;
using System;
using System.Data;
using System.Diagnostics;

namespace Nemo
{
    internal static class DataSetUtil
    {
        internal static void CheckArgumentNull<T>(T argumentValue, string argumentName) where T : class
        {
            if (null == argumentValue)
            {
                throw ArgumentNull(argumentName);
            }
        }
        
        private static T TraceExceptionAsReturnValue<T>(T e)
            where T : Exception
        {
            Debug.Assert(e != null, "TraceException: null Exception");
            if (e == null) return null;

            Debug.Assert(IsCatchableExceptionType(e), "Invalid exception type, should have been re-thrown!");
            Log.Capture(e);

            return e;
        }

        internal static ArgumentException Argument(string message)
        {
            return TraceExceptionAsReturnValue(new ArgumentException(message));
        }

        internal static ArgumentNullException ArgumentNull(string message)
        {
            return TraceExceptionAsReturnValue(new ArgumentNullException(message));
        }

        internal static ArgumentOutOfRangeException ArgumentOutOfRange(string message, string parameterName)
        {
            return TraceExceptionAsReturnValue(new ArgumentOutOfRangeException(parameterName, message));
        }

        internal static InvalidCastException InvalidCast(string message)
        {
            return TraceExceptionAsReturnValue(new InvalidCastException(message));
        }

        internal static InvalidOperationException InvalidOperation(string message)
        {
            return TraceExceptionAsReturnValue(new InvalidOperationException(message));
        }

        internal static NotSupportedException NotSupported(string message)
        {
            return TraceExceptionAsReturnValue(new NotSupportedException(message));
        }

        internal static ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
        {
            return ArgumentOutOfRange($"The {type.Name} enumeration value, {value.ToString(System.Globalization.CultureInfo.InvariantCulture)}, is not valid.", type.Name);
        }

        internal static ArgumentOutOfRangeException InvalidDataRowState(DataRowState value)
        {
#if DEBUG
            switch (value)
            {
                case DataRowState.Detached:
                case DataRowState.Unchanged:
                case DataRowState.Added:
                case DataRowState.Deleted:
                case DataRowState.Modified:
                    Debug.Assert(false, "valid DataRowState " + value);
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(DataRowState), (int)value);
        }

        internal static ArgumentOutOfRangeException InvalidLoadOption(LoadOption value)
        {
#if DEBUG
            switch (value)
            {
                case LoadOption.OverwriteChanges:
                case LoadOption.PreserveChanges:
                case LoadOption.Upsert:
                    Debug.Assert(false, "valid LoadOption " + value);
                    break;
            }
#endif
            return InvalidEnumerationValue(typeof(LoadOption), (int)value);
        }

        // only StackOverflowException & ThreadAbortException are sealed classes
        private static readonly Type SStackOverflowType = typeof(StackOverflowException);
        private static readonly Type SOutOfMemoryType = typeof(OutOfMemoryException);
        private static readonly Type SThreadAbortType = typeof(System.Threading.ThreadAbortException);
        private static readonly Type SNullReferenceType = typeof(NullReferenceException);
        private static readonly Type SAccessViolationType = typeof(AccessViolationException);
        private static readonly Type SSecurityType = typeof(System.Security.SecurityException);

        internal static bool IsCatchableExceptionType(Exception e)
        {
            // a 'catchable' exception is defined by what it is not.
            var type = e.GetType();

            return ((type != SStackOverflowType) &&
                    (type != SOutOfMemoryType) &&
                    (type != SThreadAbortType) &&
                    (type != SNullReferenceType) &&
                    (type != SAccessViolationType) &&
                    !SSecurityType.IsAssignableFrom(type));
        }
    }
}
