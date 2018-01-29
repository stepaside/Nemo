using System;

namespace Nemo.Attributes.Converters
{
	public class CastFromObjectConverter<T> : ITypeConverter<object, T>
	{
		#region ITypeConverter<F,T> Members
		
        T ITypeConverter<object, T>.ConvertForward(object from)
		{
			return (T)from;
		}

		object ITypeConverter<object, T>.ConvertBackward(T to)
		{
			return to;
		}
		
        #endregion
	}
}