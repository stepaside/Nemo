using System;

namespace Nemo.Attributes.Converters
{
	public class DBNullableTypeConverter<T> : ITypeConverter<object, T?> 
        where T : struct
	{
		#region ITypeConverter<object,T?> Members
		
        T? ITypeConverter<object, T?>.ConvertForward(object from)
		{
			if (from == null || from is DBNull)
			{
				return null;
			}
			else
			{
				return (T)from;
			}
		}

		object ITypeConverter<object, T?>.ConvertBackward(T? to)
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