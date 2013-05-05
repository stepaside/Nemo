using System;

namespace Nemo.Attributes.Converters
{
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