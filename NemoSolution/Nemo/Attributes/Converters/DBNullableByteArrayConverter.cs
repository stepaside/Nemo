using System;

namespace Nemo.Attributes.Converters
{
    public class DBNullableByteArrayConverter : ITypeConverter<object, byte[]>
    {
        #region ITypeConverter<object,string> Members

        byte[] ITypeConverter<object, byte[]>.ConvertForward(object from)
        {
            if (from == null || from is DBNull)
            {
                return null;
            }
            return (byte[])from;
        }

        object ITypeConverter<object, byte[]>.ConvertBackward(byte[] to)
        {
            if (to != null)
            {
                return to;
            }
            return DBNull.Value;
        }
        
        #endregion
    }
}