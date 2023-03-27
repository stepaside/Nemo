using Nemo.Reflection;
using System;

namespace Nemo.Attributes.Converters
{
	public class SimpleTypeConverter<T> : ITypeConverter<object, T>
    {
        private readonly bool _throwOnFailure;

        public SimpleTypeConverter()
        { }

        public SimpleTypeConverter(bool throwOnFailure)
        {
            _throwOnFailure = throwOnFailure;
        }

        #region ITypeConverter<F,T> Members

        T ITypeConverter<object, T>.ConvertForward(object from)
        {
            if (from == null || from == DBNull.Value)
            {
                return default;
            }

            if (from.GetType() == typeof(T))
            {
                return (T)from;
            }

            var type = Reflector.GetReflectedType<T>();
            if (type.IsSimpleType)
            {
                var targetType = typeof(T);
                if (targetType == typeof(bool))
                {
                    if (from is string str)
                    {
                        var fromValue = str.ToUpper();
                        if (fromValue == "N" || fromValue == "NO" || fromValue == "F" || fromValue == "FALSE" || fromValue == "0")
                        {
                            return (T)(object)false;
                        }
                        else if (fromValue == "Y" || fromValue == "YES" || fromValue == "T" || fromValue == "TRUE" || fromValue == "1")
                        {
                            return (T)(object)true;
                        }
                    }
                    else if (from is char ch)
                    {
                        var fromValue = char.ToUpper(ch);
                        if (fromValue == 'N' || fromValue == 'F' || fromValue == '0')
                        {
                            return (T)(object)false;
                        }
                        else if (fromValue == 'Y' || fromValue == 'T' || fromValue == '1')
                        {   
                            return (T)(object)true;
                        }
                    }
                    return (T)(object)Convert.ToBoolean(from);
                }
                else if (targetType == typeof(sbyte))
                {
                    return (T)(object)Convert.ToSByte(from);
                }
                else if (targetType == typeof(byte))
                {
                    return (T)(object)Convert.ToSByte(from);
                }
                else if (targetType == typeof(short))
                {
                    return (T)(object)Convert.ToInt16(from);
                }
                else if (targetType == typeof(ushort))
                {
                    return (T)(object)Convert.ToUInt16(from);
                }
                else if (targetType == typeof(int))
                {
                    return (T)(object)Convert.ToInt32(from);
                }
                else if (targetType == typeof(uint))
                {
                    return (T)(object)Convert.ToUInt32(from);
                }
                else if (targetType == typeof(long))
                {
                    return (T)(object)Convert.ToInt64(from);
                }
                else if (targetType == typeof(ulong))
                {
                    return (T)(object)Convert.ToUInt64(from);
                }
                else if (targetType == typeof(float))
                {
                    return (T)(object)Convert.ToSingle(from);
                }
                else if (targetType == typeof(double))
                {
                    return (T)(object)Convert.ToDouble(from);
                }
                else if (targetType == typeof(decimal))
                {
                    return (T)(object)Convert.ToDecimal(from);
                }
                else if (targetType == typeof(DateTime))
                {
                    return (T)(object)Convert.ToDateTime(from);
                }
                else if (targetType == typeof(DateTimeOffset))
                {
                    if  (DateTimeOffset.TryParse(Convert.ToString(from), out var value))
                    {
                        return (T)(object)value;
                    }
                }
                    else if (targetType == typeof(DateTimeOffset))
                {
                    if  (TimeSpan.TryParse(Convert.ToString(from), out var value))
                    {
                        return (T)(object)value;
                    }
                }
                else if (targetType == typeof(char))
                {
                    return (T)(object)Convert.ToChar(from);
                }
                else if (targetType == typeof(string))
                {
                    return (T)(object)Convert.ToString(from);
                }
                else if (targetType == typeof(Guid))
                {
                    Guid value;
                    if (Guid.TryParse(Convert.ToString(from), out value))
                    {
                        return (T)(object)value;
                    }
                }
                else if (targetType.IsEnum)
                {
                    var value = Enum.Parse(targetType, Convert.ToString(from));
                    if (Enum.IsDefined(targetType, value))
                    {
                        return (T)value;
                    }
                }
            }

            if (!_throwOnFailure)
            {
                return default;
            }
            throw new InvalidCastException();
        }

		object ITypeConverter<object, T>.ConvertBackward(T to)
		{
			return to;
		}

		#endregion
	}

    public class ThrowingSimpleTypeConverter<T> : ITypeConverter<object, T>
    {
        private readonly ITypeConverter<object, T> _internal = new SimpleTypeConverter<T>(true);

        #region ITypeConverter<F,T> Members

        T ITypeConverter<object, T>.ConvertForward(object from)
        {
            return _internal.ConvertForward(from);
        }

        object ITypeConverter<object, T>.ConvertBackward(T to)
        {
            return _internal.ConvertBackward(to);
        }

        #endregion
    }
}