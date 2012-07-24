using System;
using System.Collections.Generic;
using System.Linq;

namespace Nemo.Attributes.Converters
{
	/// <summary>Represents a type converter casting comma-delimited strings to strongly-typed lists.</summary>
	/// <typeparam name="T"></typeparam>
	public class ListConverter<T> : ITypeConverter<object, List<T>>
	{
		#region ITypeConverter<F,List<T>> Members
		List<T> ITypeConverter<object, List<T>>.ConvertForward(object from)
		{
			if ((!typeof(T).IsValueType && typeof(T) != typeof(string)) || from == null || from == DBNull.Value)
			{
				return null;
			}

			return from.ToString().Split(',').Where(v => !string.IsNullOrEmpty(v)).Select(v => (T)Convert.ChangeType(v.Trim(), typeof(T))).ToList();
		}

		object ITypeConverter<object, List<T>>.ConvertBackward(List<T> to)
		{
			if ((!typeof(T).IsValueType && typeof(T) != typeof(string)) || to == null)
			{
				return null;
			}

			return string.Join(",", to.Select(v => v.ToString()).ToArray());
		}
		#endregion
	}
}