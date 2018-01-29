using System;

namespace Nemo.Attributes.Converters
{
	public class NullableEnumConverter<T> : ITypeConverter<string, T?>
		where T : struct //	Should actually be an enum
	{
		private ITypeConverter<string, T> decoratedConverter = new EnumConverter<T>();

		#region ITypeConverter<string,T?> Members
		T? ITypeConverter<string, T?>.ConvertForward(string from)
		{
			if (string.IsNullOrEmpty(from))
			{
				return null;
			}
			else
			{
				return decoratedConverter.ConvertForward(from);
			}
		}

		string ITypeConverter<string, T?>.ConvertBackward(T? to)
		{
			if (to == null)
			{
				return string.Empty;
			}
			else
			{
				return decoratedConverter.ConvertBackward(to.Value);
			}
		}
		#endregion
	}
}