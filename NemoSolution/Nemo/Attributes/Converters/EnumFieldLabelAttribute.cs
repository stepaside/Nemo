using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;

namespace Nemo.Attributes.Converters
{
	/// <summary>Associates a <see cref="string"/> label to an enum field.</summary>
	[AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
	public class EnumFieldLabelAttribute : Attribute
	{
		private readonly string label;

		/// <summary>Construct the value object attribute.</summary>
		/// <param name="label"></param>
		public EnumFieldLabelAttribute(string label)
		{
			this.label = label;
		}

		/// <summary>Label associate to an enum field.</summary>
		public string Label
		{
			get { return label; }
		}

		/// <summary>Returns a mapping from enum values to strings.</summary>
		/// <param name="enumType"></param>
		/// <returns></returns>
		public static IDictionary<object, string> GetLabelMapping(Type enumType)
		{
			//	Get the enum fields
			FieldInfo[] fields = enumType.GetFields(BindingFlags.Public | BindingFlags.Static);
			Array values = Enum.GetValues(enumType);
			Dictionary<object, string> map = new Dictionary<object, string>(fields.Length);

			for (int i = 0; i != fields.Length; ++i)
			{
				EnumFieldLabelAttribute labelAttribute = (EnumFieldLabelAttribute)Attribute.GetCustomAttribute(
					fields[i],
					typeof(EnumFieldLabelAttribute));

				if (labelAttribute == null)
				{
					map.Add(values.GetValue(i), values.GetValue(i).ToString());
				}
				else
				{
					map.Add(values.GetValue(i), labelAttribute.Label);
				}
			}

			return map;
		}
	}
}