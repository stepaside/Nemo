using System;

namespace Nemo.Attributes.Converters
{
	/// <summary>
	/// Represents a type converter converting objects to nullable types.  If the object is
	/// <see cref="DBNull"/>, the nullable type will be <c>null</c>.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class DBNullableTypeConverter<T> : ITypeConverter<object, Nullable<T>> where T : struct
	{
		#region ITypeConverter<object,T?> Members
		T? ITypeConverter<object, T?>.ConvertForward(object from)
		{
			if (from == null || from is DBNull)
			{
				return null;
			}
			else
			{
				return (T)from;
			}
		}

		object ITypeConverter<object, T?>.ConvertBackward(T? to)
		{
			if (to != null)
			{
				return to;
			}
			else
			{
				return DBNull.Value;
			}
		}
		#endregion
	}
}