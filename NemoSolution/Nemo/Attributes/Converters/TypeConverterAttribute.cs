using System;
using System.Linq;
using System.Reflection;

namespace Nemo.Attributes.Converters
{
	/// <summary>Associate type converter types list to a property.</summary>
	[AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = true)]
	public class TypeConverterAttribute : Attribute
	{
		private readonly Type _typeConverterType;

		/// <summary>Associate a list of type-converter type with the attribute.</summary>
		/// <remarks>
		/// The converter type list can be <c>empty</c>, in which case it means there should be no conversion.
		/// </remarks>
		/// <param name="typeConverterTypeList"></param>
		public TypeConverterAttribute(params Type[] typeConverterTypes)
		{
			if (typeConverterTypes == null || typeConverterTypes.Length == 0)
			{
				_typeConverterType = null;
			}
			else if (typeConverterTypes.Length == 1)
			{
				_typeConverterType = typeConverterTypes[0];
			}
			else
			{	//	Compose the converters into one
				int i = 1;

				_typeConverterType = typeConverterTypes[0];
				while (i < typeConverterTypes.Length)
				{
					_typeConverterType = ComposeConverters(_typeConverterType, typeConverterTypes[i]);
					++i;
				}
			}
		}

		/// <summary>Converter type associated with this attribute.</summary>
		public Type TypeConverterType
		{
			get { return _typeConverterType; }
		}

		/// <summary>Returns the custom attributes associated with a property in a given context.</summary>
		/// <param name="property"></param>
		/// <returns></returns>
		public static TypeConverterAttribute GetTypeConverter(PropertyInfo property)
		{
			return property.GetCustomAttributes(typeof(TypeConverterAttribute), false).Cast<TypeConverterAttribute>().FirstOrDefault();
		}

		/// <summary>Returns the expected converter interface type, given from and to types.</summary>
		/// <param name="fromType"></param>
		/// <param name="toType"></param>
		/// <returns></returns>
		public static Type GetExpectedConverterInterfaceType(Type fromType, Type toType)
		{
			Type genericType = typeof(ITypeConverter<,>);
			Type expectedInterfaceType = genericType.MakeGenericType(fromType, toType);

			return expectedInterfaceType;
		}

		/// <summary>Validates that the type converter can work given from and to types.</summary>
		/// <param name="fromType"></param>
		/// <param name="toType"></param>
		public void ValidateTypeConverterType(Type fromType, Type toType)
		{
			if (TypeConverterType == null)
			{
				if (fromType != toType && fromType.IsAssignableFrom(toType))
				{
					throw new TypeConverterException(string.Format("No type converter were enforced, but there should be one from type {0} to type {1}.", fromType.FullName, toType.FullName));
				}
			}
			else
			{
				Type expectedInterfaceType = GetExpectedConverterInterfaceType(fromType, toType);
				Type[] converterIntefaces = TypeConverterType.GetInterfaces();

				if (TypeConverterType.IsAbstract)
				{
					throw new TypeConverterException(string.Format("Can't use {0} as a converter because it is abstract.", TypeConverterType.FullName));
				}
				if (TypeConverterType.GetConstructor(Type.EmptyTypes) == null)
				{
					throw new TypeConverterException(string.Format("Can't use {0} as a converter because it doesn't have a default constructor.", TypeConverterType.FullName));
				}
				if (Array.IndexOf<Type>(converterIntefaces, expectedInterfaceType) == -1)
				{
					throw new TypeConverterException(string.Format("Can't use {0} as a converter because it doesn't implement {1}.", TypeConverterType.FullName, expectedInterfaceType));
				}
			}
		}

		private static Type GetConverterInterfaceType(Type typeConverterType)
		{
			Type[] converterInterfaces = typeConverterType.GetInterfaces();

			foreach (Type interfaceType in converterInterfaces)
			{
				if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(ITypeConverter<,>))
				{
					return interfaceType;
				}
			}

			return null;
		}

		/// <summary>Composes two converters and returns the resulting converter type.</summary>
		/// <param name="typeConverterType1"></param>
		/// <param name="typeConverterType2"></param>
		/// <returns></returns>
		private static Type ComposeConverters(Type typeConverterType1, Type typeConverterType2)
		{
			Type interfaceType1 = GetConverterInterfaceType(typeConverterType1);
			Type interfaceType2 = GetConverterInterfaceType(typeConverterType2);

			if (interfaceType1 == null)
			{
				throw new TypeConverterException(string.Format("Can't use type {0} for type converter:  it isn't a type converter.", typeConverterType1.FullName));
			}
			if (interfaceType2 == null)
			{
				throw new TypeConverterException(string.Format("Can't use type {0} for type converter:  it isn't a type converter.", typeConverterType2.FullName));
			}
			if (interfaceType1.GetGenericArguments()[1] != interfaceType2.GetGenericArguments()[0])
			{
				throw new TypeConverterException(string.Format("Can't compose the following two type converters:  {0} & {1}.", typeConverterType1.FullName, typeConverterType2.FullName));
			}

			Type from = interfaceType1.GetGenericArguments()[0];
			Type intermediate = interfaceType1.GetGenericArguments()[1];
			Type to = interfaceType2.GetGenericArguments()[1];
			Type genericResult = typeof(CompositeConverter<,,,,>);
			Type result = genericResult.MakeGenericType(typeConverterType1, typeConverterType2, from, intermediate, to);

			return result;
		}
	}
}
