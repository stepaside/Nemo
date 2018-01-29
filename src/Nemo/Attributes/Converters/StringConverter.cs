using System;

namespace Nemo.Attributes.Converters
{
	public class StringConverter<T> : ITypeConverter<string, T>
	{
        private ITypeConverter<object, T> decoratedConverter = new SimpleTypeConverter<T>();

		#region ITypeConverter<F,T> Members
		
        T ITypeConverter<string, T>.ConvertForward(string from)
		{
            return decoratedConverter.ConvertForward(from);
		}

		string ITypeConverter<string, T>.ConvertBackward(T to)
		{
			return Convert.ToString(to);
		}

		#endregion
	}
}