using System;

namespace Nemo.Attributes.Converters
{
	public class NullableEnumConverter<T> : ITypeConverter<object, T?>
		where T : struct //	Should actually be an enum
	{
		private readonly ITypeConverter<object, T> decoratedConverter = new EnumConverter<T>();

		#region ITypeConverter<string,T?> Members

		T? ITypeConverter<object, T?>.ConvertForward(object from)
		{
			if (from == null)
			{
				return null;
			}
			else
			{
				return decoratedConverter.ConvertForward(from);
			}
		}

		object ITypeConverter<object, T?>.ConvertBackward(T? to)
		{
			if (to == null)
			{
				return null;
			}
			else
			{
				return decoratedConverter.ConvertBackward(to.Value);
			}
		}

		#endregion
	}
}