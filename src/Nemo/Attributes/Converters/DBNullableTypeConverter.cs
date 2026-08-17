using System;

namespace Nemo.Attributes.Converters
{
	public class DBNullableTypeConverter<T> : ITypeConverter<object, T?> 
        where T : struct
	{
        private static readonly ITypeConverter<object, T> ValueConverter = new SimpleTypeConverter<T>(true);

		#region ITypeConverter<object,T?> Members
		
        T? ITypeConverter<object, T?>.ConvertForward(object from)
		{
			if (from == null || from is DBNull)
			{
				return null;
			}
			else if (from is T value)
			{
				return value;
			}
			else
			{
				return ValueConverter.ConvertForward(from);
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