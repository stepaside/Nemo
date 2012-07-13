using System;

namespace Nemo.Attributes.Converters
{
	/// <summary>Represents a type converter casting objects to other types.</summary>
	/// <typeparam name="T"></typeparam>
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