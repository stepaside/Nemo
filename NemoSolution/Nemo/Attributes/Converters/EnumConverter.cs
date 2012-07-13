using System;
using System.Collections.Generic;

namespace Nemo.Attributes.Converters
{
	/// <summary>Represents a type converter converting strings to enums.</summary>
	/// <remarks>Works with <see cref="EnumFieldLabelAttribute"/> to override an enum string representation.</remarks>
	/// <exception cref="ArgumentNullException">If the string is null or empty.</exception>
	/// <exception cref="ArgumentException">If the string doesn't correspond to an enum of that type.</exception>
	/// <typeparam name="T"></typeparam>
	public class EnumConverter<T> : ITypeConverter<string, T> where T : struct //	Should actually be an enum
	{
		private static readonly IDictionary<object, string> enumToStringMap;
		private static readonly IDictionary<string, object> stringToEnumMap;

		/// <summary>
		/// Initializes the mappings of enum to strings, reading the <see cref="EnumFieldLabelAttribute"/>,
		/// if present.
		/// </summary>
		static EnumConverter()
		{
			enumToStringMap = EnumFieldLabelAttribute.GetLabelMapping(typeof(T));
			stringToEnumMap = new Dictionary<string, object>();

			//	Reverse the dictionary
			foreach (KeyValuePair<object, string> pair in enumToStringMap)
			{
				stringToEnumMap.Add(pair.Value, pair.Key);
			}
		}

		#region ITypeConverter<object,T> Members
		T ITypeConverter<string, T>.ConvertForward(string from)
		{
			if (string.IsNullOrEmpty(from))
			{
				throw new ArgumentNullException("from");
			}

			return (T)stringToEnumMap[from];
		}

		string ITypeConverter<string, T>.ConvertBackward(T to)
		{
			return enumToStringMap[to];
		}
		#endregion
	}
}