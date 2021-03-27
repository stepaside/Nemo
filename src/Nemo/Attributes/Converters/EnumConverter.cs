using System;
using System.Collections.Generic;

namespace Nemo.Attributes.Converters
{
	public class EnumConverter<T> : ITypeConverter<object, T> 
        where T : struct
	{
		#region ITypeConverter<object,T> Members
		
        T ITypeConverter<object, T>.ConvertForward(object from)
		{
			switch(from)
            {
				case T t:
					return t;
				case int i:
					return (T)Enum.ToObject(typeof(T), i);
				case long l:
					return (T)Enum.ToObject(typeof(T), l);
				case short s:
					return (T)Enum.ToObject(typeof(T), s);
				case ushort us:
					return (T)Enum.ToObject(typeof(T), us);
				case ulong ul:
					return (T)Enum.ToObject(typeof(T), ul);
				case uint ui:
					return (T)Enum.ToObject(typeof(T), ui);
				case byte b:
					return (T)Enum.ToObject(typeof(T), b);
				case sbyte sb:
					return (T)Enum.ToObject(typeof(T), sb);
			}

			if (!Enum.TryParse<T>(Convert.ToString(from), out var result))
            {
                result = default;
            }

			return result;
		}

		object ITypeConverter<object, T>.ConvertBackward(T to)
		{
			return to;
		}
		
        #endregion
	}
}