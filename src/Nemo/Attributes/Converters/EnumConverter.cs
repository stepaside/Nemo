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
            if (!Enum.TryParse<T>(Convert.ToString(from), out var result))
            {
                result = default;
            }

			return result;
		}

		object ITypeConverter<object, T>.ConvertBackward(T to)
		{
			return to.ToString();
		}
		
        #endregion
	}
}