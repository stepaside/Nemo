using System;

namespace Nemo.Attributes.Converters
{
    /// <summary>
    /// Represents a type converter converting strings to nullable strings.  If the object is
    /// <see cref="DBNull"/>, the nullable type will be <c>null</c> string.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DBNullableStringConverter : ITypeConverter<object, string>
    {
        #region ITypeConverter<object,string> Members
        string ITypeConverter<object, string>.ConvertForward(object from)
        {
            if (from == null || from is DBNull)
            {
                return null;
            }
            else
            {
                return Convert.ToString(from);
            }
        }

        object ITypeConverter<object, string>.ConvertBackward(string to)
        {
            if (to != null)
            {
                return to;
            }
            else
            {
                return DBNull.Value;
            }
        }
        #endregion
    }
}