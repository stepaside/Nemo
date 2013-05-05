using System;

namespace Nemo.Attributes.Converters
{
	public class ObjectToValueTypeConverter<T> : ITypeConverter<object, T>
    {
		#region ITypeConverter<F,T> Members
	
        T ITypeConverter<object, T>.ConvertForward(object from)
		{
			if (from == null || from == DBNull.Value)
			{
				return default(T);
			}

			var targetType = typeof(T);
			if (targetType == typeof(bool))
			{
				if (from is string)
				{
					string fromValue = from.ToString().ToUpper();
					if (fromValue == "N" || fromValue == "NO" || fromValue == "F" || fromValue == "FALSE" || fromValue == "0")
					{
						return (T)((object)false);
					}
					else if (fromValue == "Y" || fromValue == "YES" || fromValue == "T" || fromValue == "TRUE" || fromValue == "1")
					{
						return (T)((object)true);
					}
				}
				return (T)((object)Convert.ToBoolean(from));
			}
			else if (targetType == typeof(sbyte))
			{
				return (T)((object)Convert.ToSByte(from));
			}
			else if (targetType == typeof(byte))
			{
				return (T)((object)Convert.ToSByte(from));
			}
			else if (targetType == typeof(short))
			{
				return (T)((object)Convert.ToInt16(from));
			}
			else if (targetType == typeof(ushort))
			{
				return (T)((object)Convert.ToUInt16(from));
			}
			else if (targetType == typeof(int))
			{
				return (T)((object)Convert.ToInt32(from));
			}
			else if (targetType == typeof(uint))
			{
				return (T)((object)Convert.ToUInt32(from));
			}
			else if (targetType == typeof(long))
			{
				return (T)((object)Convert.ToInt64(from));
			}
			else if (targetType == typeof(ulong))
			{
				return (T)((object)Convert.ToUInt64(from));
			}
			else if (targetType == typeof(float))
			{
				return (T)((object)Convert.ToSingle(from));
			}
			else if (targetType == typeof(double))
			{
				return (T)((object)Convert.ToDouble(from));
			}
			else if (targetType == typeof(decimal))
			{
				return (T)((object)Convert.ToDecimal(from));
			}
			else if (targetType == typeof(DateTime))
			{
				return (T)((object)Convert.ToDateTime(from));
			}
			else if (targetType == typeof(char))
			{
				return (T)((object)Convert.ToChar(from));
			}
			else if (targetType == typeof(string))
			{
				return (T)((object)Convert.ToString(from));
			}
			else if (targetType.IsEnum)
			{
				object value = Enum.Parse(targetType, Convert.ToString(from));
				if (Enum.IsDefined(targetType, value))
				{
					return (T)value;
				}
				else
				{
					return default(T);
				}
			}
			else
			{
				return default(T);
			}
		}

		object ITypeConverter<object, T>.ConvertBackward(T to)
		{
			return to;
		}

		#endregion
	}
}