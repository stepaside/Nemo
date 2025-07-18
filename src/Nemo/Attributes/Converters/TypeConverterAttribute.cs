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
			return property?.GetCustomAttributes(typeof(TypeConverterAttribute), false).Cast<TypeConverterAttribute>().FirstOrDefault();
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
	    /// <param name="converterType"></param>
	    /// <param name="fromType"></param>
	    /// <param name="toType"></param>
	    internal static void ValidateTypeConverterType(Type converterType, Type fromType, Type toType)
		{
            if (converterType == null)
			{
				if (fromType != toType && fromType.IsAssignableFrom(toType))
				{
					throw new TypeConverterException($"No type converter were enforced, but there should be one from type {fromType.FullName} to type {toType.FullName}.");
				}
			}
			else
			{
                if (fromType == null || toType == null)
				{
					throw new TypeConverterException($"Can't use {converterType.FullName} as a converter because from or to type is null.");
				}

				var expectedInterfaceType = GetExpectedConverterInterfaceType(fromType, toType);
                var converterIntefaces = converterType.GetInterfaces();

                if (converterType.IsAbstract)
				{
                    throw new TypeConverterException($"Can't use {converterType.FullName} as a converter because it is abstract.");
				}
                if (converterType.GetConstructor(Type.EmptyTypes) == null)
				{
                    throw new TypeConverterException($"Can't use {converterType.FullName} as a converter because it doesn't have a default constructor.");
				}
				if (Array.IndexOf(converterIntefaces, expectedInterfaceType) == -1)
				{
                    throw new TypeConverterException($"Can't use {converterType.FullName} as a converter because it doesn't implement {expectedInterfaceType.FullName}.");
				}
			}
		}

		private static Type GetConverterInterfaceType(Type typeConverterType)
		{
			var converterInterfaces = typeConverterType.GetInterfaces();

		    return converterInterfaces.FirstOrDefault(interfaceType => interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(ITypeConverter<,>));
		}

		/// <summary>Composes two converters and returns the resulting converter type.</summary>
		/// <param name="typeConverterType1"></param>
		/// <param name="typeConverterType2"></param>
		/// <returns></returns>
		internal static Type ComposeConverters(Type typeConverterType1, Type typeConverterType2)
		{
			var interfaceType1 = GetConverterInterfaceType(typeConverterType1);
			var interfaceType2 = GetConverterInterfaceType(typeConverterType2);

			if (interfaceType1 == null)
			{
				throw new TypeConverterException($"Can't use type {typeConverterType1.FullName} for type converter:  it isn't a type converter.");
			}
			if (interfaceType2 == null)
			{
				throw new TypeConverterException($"Can't use type {typeConverterType2.FullName} for type converter:  it isn't a type converter.");
			}
			if (interfaceType1.GetGenericArguments()[1] != interfaceType2.GetGenericArguments()[0])
			{
				throw new TypeConverterException($"Can't compose the following two type converters:  {typeConverterType1.FullName} & {typeConverterType2.FullName}.");
			}

			var from = interfaceType1.GetGenericArguments()[0];
			var intermediate = interfaceType1.GetGenericArguments()[1];
			var to = interfaceType2.GetGenericArguments()[1];
			var genericResult = typeof(CompositeConverter<,,,,>);
			var result = genericResult.MakeGenericType(typeConverterType1, typeConverterType2, from, intermediate, to);

			return result;
		}

        internal static Tuple<Type, Type> GetTypeConverter(Type indexerType, PropertyInfo property)
        {
            var typeConverterAttribute = GetTypeConverter(property);

            Type typeConverterType = null;
            Type typeConverterInterfaceType = null;

            if (typeConverterAttribute != null && typeConverterAttribute.TypeConverterType != null)
            {
                var propertyType = property?.PropertyType;
                ValidateTypeConverterType(typeConverterAttribute.TypeConverterType, indexerType, propertyType);
                typeConverterType = typeConverterAttribute.TypeConverterType;
                typeConverterInterfaceType = GetExpectedConverterInterfaceType(indexerType, propertyType);
            }

            return Tuple.Create(typeConverterType, typeConverterInterfaceType);
        }

	}
}
