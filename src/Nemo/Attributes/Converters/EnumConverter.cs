using System;
using System.Collections.Generic;

namespace Nemo.Attributes.Converters
{
	public class EnumConverter<T> : ITypeConverter<string, T> 
        where T : struct //	Should actually be an enum
	{
		#region ITypeConverter<object,T> Members
		
        T ITypeConverter<string, T>.ConvertForward(string from)
		{
            T result;
            if (!Enum.TryParse<T>(from, out result))
            {
                result = default(T);
            }

			return result;
		}

		string ITypeConverter<string, T>.ConvertBackward(T to)
		{
			return to.ToString();
		}
		
        #endregion
	}
}