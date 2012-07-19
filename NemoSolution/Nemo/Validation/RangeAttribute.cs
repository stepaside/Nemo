using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Reflection;

namespace Nemo.Validation
{
    public class RangeAttribute : System.ComponentModel.DataAnnotations.RangeAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        public RangeAttribute(double minimum, double maximum) : base(minimum, maximum) { }
        public RangeAttribute(int minimum, int maximum) : base(minimum, maximum) { }
        public RangeAttribute(Type type, string minimum, string maximum) : base(type, minimum, maximum) { }
        public RangeAttribute(Type type, string range) : base (type, GetMinimum(type, range), GetMaximum(type, range)) { }

        public SeverityType SeverityType
        {
            get;
            set;
        }

        private string _resourceKey = null;
        public string ResourceKey
        {
            get
            {
                return _resourceKey;
            }
            set
            {
                _resourceKey = value;
                this.SetErrorMessage();
            }
        }

        private static string GetMinimum(Type type, string range)
        {
            bool inclusive = range.StartsWith("[");
            return GetLimit(type, range, inclusive, false);
        }

        private static string GetMaximum(Type type, string range)
        {
            bool inclusive = range.EndsWith("]");
            return GetLimit(type, range, inclusive, true);
        }

        private static string GetLimit(Type type, string range, bool inclusive, bool upperLimit)
        {
            string[] parts = range.TrimStart('[', '(').TrimEnd(')', ']').Split(',');
            if (parts.Length == 2)
            {
                object value = Convert.ChangeType(parts[(upperLimit ? 1 : 0)].Trim(), type);
                if (!inclusive)
                {
                    value = Modify(type, value, false);
                }
                return value.ToString();
            }
            else
            {
                throw new ApplicationException("Invalid range specified.");
            }
        }

        private static object Modify(Type type, object value, bool increment)
        {
            if (type == typeof(int))
            {
                value = ((int)value) + (increment ? 1 : -1);
            }
            else if (type == typeof(short))
            {
                value = ((short)value) + (increment ? 1 : -1);
            }
            else if (type == typeof(long))
            {
                value = ((long)value) + (increment ? 1 : -1);
            }
            else if (type == typeof(DateTime))
            {
                value = ((DateTime)value).AddMilliseconds(increment ? double.Epsilon : -double.Epsilon);
            }
            else if (type == typeof(DateTimeOffset))
            {
                value = ((DateTimeOffset)value).AddMilliseconds(increment ? double.Epsilon : -double.Epsilon);
            }
            else if (type == typeof(double))
            {
                value = ((double)value) + (increment ? double.Epsilon : -double.Epsilon);
            }
            else if (type == typeof(float))
            {
                value = ((float)value) + (increment ? float.Epsilon : -float.Epsilon);
            }

            return value;
        }
    }
}
