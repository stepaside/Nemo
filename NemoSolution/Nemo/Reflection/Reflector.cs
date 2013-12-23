using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Xml.Linq;
using Nemo.Attributes;
using Nemo.Caching;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Serialization;
using Nemo.Utilities;
using Nemo.Collections;
using System.Collections.ObjectModel;
using Nemo.Configuration.Mapping;

namespace Nemo.Reflection
{
    public static partial class Reflector
    {
        #region Declarations

        private static readonly MethodInfo _getReflectedTypeMethod = typeof(Reflector).GetMethod("GetReflectedType", BindingFlags.Static | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
        private static readonly MethodInfo _getPropertyMapMethod = typeof(Reflector).GetMethod("GetPropertyMap", BindingFlags.Static | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
        private static readonly MethodInfo _getPropertyNameMapMethod = typeof(Reflector).GetMethod("GetPropertyNameMap", BindingFlags.Static | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
        private static readonly MethodInfo _getAllPropertiesMethod = typeof(Reflector).GetMethod("GetAllProperties", Type.EmptyTypes);
        private static readonly MethodInfo _getAllPropertyPositionsMethod = typeof(Reflector).GetMethod("GetAllPropertyPositions", Type.EmptyTypes);
        private static readonly MethodInfo _getPropertyMethod = typeof(Reflector).GetMethod("GetProperty", new Type[] { typeof(string) });
        private static readonly MethodInfo _extractInterfaceMethod = typeof(Reflector).GetMethod("ExtractInterface", Type.EmptyTypes);
        private static readonly MethodInfo _extractInterfacesMethod = typeof(Reflector).GetMethod("ExtractIntefaces", Type.EmptyTypes);

        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getReflectedTypeCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getPropertyMapCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getPropertyNameMapCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getAllPropertiesCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getAllPropertyPositionsCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _getPropertyCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _extractInterfaceCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _extractInterfacesCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        private static ConcurrentDictionary<Type, RuntimeMethodHandle> _defaultConstructors = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        private static ConcurrentDictionary<Type, bool> _collectionTypes = new ConcurrentDictionary<Type, bool>();

        private static readonly HashSet<Type> _genericTupleTypes = new HashSet<Type>(new Type[] { typeof(Tuple<>), typeof(Tuple<,>), 
                                                                                                typeof(Tuple<,,>), typeof(Tuple<,,,>), 
                                                                                                typeof(Tuple<,,,,>), typeof(Tuple<,,,,,>), 
                                                                                                typeof(Tuple<,,,,,,>), typeof(Tuple<,,,,,,,>) });
        #endregion

        public static bool InheritsFrom(this Type thisType, Type baseType)
        {
            return baseType.IsAssignableFrom(thisType);
        }

        public static object GetDefault(this Type type)
        {
            return type.IsValueType ? System.Activator.CreateInstance(type) : null;
        }

        public static object ChangeType(object value, Type conversionType)
        {
            if (conversionType == null)
            {
                throw new ArgumentNullException("conversionType");
            }

            var newConversionType = Nullable.GetUnderlyingType(conversionType);

            if (newConversionType != null)
            {
                conversionType = newConversionType;
                if (value == null)
                {
                    return null;
                }
            }

            return Convert.ChangeType(value, conversionType);
        }

        public static bool IsComparable(object objectValue)
        {
            return objectValue is IComparable;
        }

        public static bool IsBusinessObject(object objectValue)
        {
            return objectValue is IBusinessObject;
        }

        public static bool IsBusinessObject(Type objectType)
        {
            return typeof(IBusinessObject).IsAssignableFrom(objectType);
        }

        public static bool IsBusinessObjectList(Type objectType, out Type elementType)
        {
            var result = false;
            if (Reflector.IsList(objectType))
            {
                elementType = Reflector.ExtractCollectionElementType(objectType);
                if (elementType != null && Reflector.IsBusinessObject(elementType))
                {
                    result = true;
                }
            }
            else
            {
                elementType = null;
            }
            return result;
        }

        public static bool IsCacheableBusinessObject(Type objectType)
        {
            return Reflector.IsBusinessObject(objectType) && ObjectCache.IsCacheable(objectType);
        }

        public static bool IsInterface(Type objectType)
        {
            return objectType != null && objectType.IsInterface;
        }

        public static bool IsValueType(Type objectType)
        {
            return objectType != null && objectType.IsValueType;
        }

        public static bool IsStringOrValueType(Type objectType)
        {
            return objectType != null && (objectType == typeof(string) || objectType.IsValueType);
        }

        public static bool IsMarkerInterface(Type objectType)
        {
            return objectType == typeof(IBusinessObject) || objectType == typeof(IChangeTrackingBusinessObject);
        }

        public static bool IsMarkerInterface<T>()
        {
            return TypeCache<T>.Type.IsMarkerInterface;
        }

        public static bool IsXmlType(Type objectType)
        {
            return objectType == typeof(XmlDocument) || objectType == typeof(XDocument) || objectType == typeof(XmlReader);
        }

        public static bool IsSimpleType(Type objectType)
        {
            return IsSimpleNullableType(objectType) || IsSimpleNonNullableType(objectType);
        }

        public static bool IsSimpleNullableType(Type objectType)
        {
            if (IsNullableType(objectType))
            {
                var genericArguments = objectType.GetGenericArguments();
                if (genericArguments.Length > 0)
                {
                    return IsSimpleNonNullableType(genericArguments[0]);
                }
            }
            return false;
        }

        public static bool IsSimpleNonNullableType(Type objectType)
        {
            return IsNonTextType(objectType) || IsTextType(objectType);
        }

        public static bool IsTextType(Type objectType)
        {
            return objectType.IsEnum
                || objectType == typeof(DateTime)
                || objectType == typeof(DateTimeOffset)
                || objectType == typeof(TimeSpan)
                || objectType == typeof(string)
                || objectType == typeof(Guid);
        }

        public static bool IsNonTextType(Type objectType)
        {
            return objectType.IsPrimitive
                || objectType == typeof(decimal);
        }

        public static bool IsNullableType(Type objectType)
        {
            //return objectType.IsGenericType && objectType.GetGenericTypeDefinition().Equals(typeof(Nullable<>));
            Type dummy;
            return IsNullableType(objectType, out dummy);
        }

        public static bool IsNullableType(Type objectType, out Type underlyingType)
        {
            underlyingType = Nullable.GetUnderlyingType(objectType);
            return underlyingType != null;
        }

        public static bool IsNumeric(Type objectType)
        {
            return objectType.IsPrimitive
                    || objectType == typeof(decimal)
                    || objectType.IsEnum
                    || IsNullableNumeric(objectType);
        }

        public static bool IsNullableNumeric(Type objectType)
        {
            if (IsNullableType(objectType))
            {
                var genericArguments = objectType.GetGenericArguments();
                if (genericArguments.Length > 0)
                {
                    return IsNumeric(genericArguments[0]);
                }
            }
            return false;
        }

        public static bool IsList(Type objectType)
        {
            if (objectType.IsGenericType)
            {
                var genericTypeDefinition = objectType.GetGenericTypeDefinition();
                if (genericTypeDefinition != typeof(List<>) && genericTypeDefinition != typeof(IList<>))
                {
                    return genericTypeDefinition.Prepend(genericTypeDefinition.GetInterfaces()).Where(s => s.IsGenericType).Select(s => s.GetGenericTypeDefinition()).Contains(typeof(IList<>));
                }
                else
                {
                    return true;
                }
            }
            else
            {
                return objectType.GetInterface("System.Collections.IList") != null;
            }
        }

        public static bool IsSimpleList(Type type)
        {
            bool result = false;
            if (IsList(type))
            {
                result = IsSimpleType(ExtractCollectionElementType(type));
            }
            return result;
        }

        public static bool IsDictionary(Type objectType)
        {
            if (objectType.IsGenericType)
            {
                var genericTypeDefinition = objectType.GetGenericTypeDefinition();
                if (genericTypeDefinition != typeof(Dictionary<,>) && genericTypeDefinition != typeof(IDictionary<,>))
                {
                    return genericTypeDefinition.Prepend(genericTypeDefinition.GetInterfaces()).Where(s => s.IsGenericType).Select(s => s.GetGenericTypeDefinition()).Contains(typeof(IDictionary<,>));
                }
                else
                {
                    return true;
                }
            }
            else
            {
                return objectType.GetInterface("System.Collections.IDictionary") != null;
            }
        }

        public static bool IsArray(Type objectType)
        {
            return objectType.IsArray;
        }

        public static bool IsTuple(Type objectType)
        {
            if (objectType.IsGenericType)
            {
                _genericTupleTypes.Contains(objectType.GetGenericTypeDefinition());
            }
            return false;
        }

        public static bool IsTypeUnion(Type objectType)
        {
            return objectType.InheritsFrom(typeof(ITypeUnion));
        }

        public static bool IsTypeUnionList(Type objectType)
        {
            var elementType = Reflector.ExtractCollectionElementType(objectType);
            if (elementType != null)
            {
                return elementType.InheritsFrom(typeof(ITypeUnion));
            }
            return false;
        }
        
        public static bool IsAnonymousType(Type objectType)
        {
            return objectType != null 
                && objectType.IsGenericType
                && (objectType.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic
                && (objectType.Name.StartsWith("<>") || objectType.Name.StartsWith("VB$", StringComparison.OrdinalIgnoreCase))
                && (objectType.Name.Contains("AnonymousType") || objectType.Name.Contains("AnonType"))
                && Attribute.IsDefined(objectType, typeof(CompilerGeneratedAttribute), false);
        }

        public static bool IsEmitted(Type objectType)
        {
            return objectType != null && objectType.Assembly.IsDynamic;
        }

        public static Type ExtractInterface(Type objectType)
        {
            var methodHandle = _extractInterfaceCache.GetOrAdd(objectType, t => _extractInterfaceMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (Type)func(null, new object[] { });
        }

        public static Type ExtractInterface<T>()
        {
            var interfaces = InterfaceCache<T>.Interfaces;
            var interfaceType = interfaces.MaxElement(k => k.Value.Count).Key;
            return interfaceType;
        }

        public static IEnumerable<Type> ExtractInterfaces(Type objectType)
        {
            var methodHandle = _extractInterfacesCache.GetOrAdd(objectType, t => _extractInterfacesMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (IEnumerable<Type>)func(null, new object[] { });
        }

        public static IEnumerable<Type> ExtractInterfaces<T>()
        {
            var interfaces = InterfaceCache<T>.Interfaces;
            return interfaces.Keys;
        }

        private static Dictionary<Type, List<Type>> CacheInterfaces<T>()
        {
            var interfaceTypes = ExtractInterfacesImplementation(typeof(T));
            var interfaces = new Dictionary<Type, List<Type>>();

            foreach (var interfaceType in interfaceTypes)
            {
                var extractedInterfaces = ExtractInterfacesImplementation(interfaceType);
                if (extractedInterfaces != null && extractedInterfaces.Any())
                {
                    interfaces[interfaceType] = extractedInterfaces.ToList();
                }
            }
            return interfaces;
        }

        private static IEnumerable<Type> ExtractInterfacesImplementation(Type type)
        {
            Type[] interfaces = type.GetInterfaces();
            Array.Reverse(interfaces);
            return (type.IsInterface ? interfaces.Append(type) : interfaces).Where(t => t != typeof(IBusinessObject) /*&& t != typeof(IChangeTrackingBusinessObject)*/ && typeof(IBusinessObject).IsAssignableFrom(t));
        }

        public static PropertyInfo[] GetAllProperties(Type objectType)
        {
            var methodHandle = _getAllPropertiesCache.GetOrAdd(objectType, t => _getAllPropertiesMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (PropertyInfo[])func(null, new object[] { });
        }

        public static PropertyInfo[] GetAllProperties<T>()
        {
            var properties = PropertyCache<T>.Properties;
            return properties.Values.ToArray();
        }

        public static PropertyInfo GetProperty(Type objectType, string name)
        {
            var methodHandle = _getPropertyCache.GetOrAdd(objectType, t => _getPropertyMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (PropertyInfo)func(null, new object[] { name });
        }

        public static PropertyInfo GetProperty<T>(string name)
        {
           var properties = PropertyCache<T>.Properties;
            PropertyInfo property;
            if (!properties.TryGetValue(name, out property))
            {
                property = null;
            }
            return property;
        }

        public static IDictionary<string, int> GetAllPropertyPositions(Type objectType)
        {
            var methodHandle = _getAllPropertyPositionsCache.GetOrAdd(objectType, t => _getAllPropertyPositionsMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (IDictionary<string, int>)func(null, new object[] { });
        }

        public static IDictionary<string, int> GetAllPropertyPositions<T>()
        {
            return PropertyCache<T>.Positions;
        }

        private static Dictionary<string, Tuple<PropertyInfo, int>> CacheProperties<T>()
        {
            var typeList = new HashSet<Type>();
            foreach (var interfaceType in Reflector.ExtractInterfaces<T>(/*objectType*/))
            {
                typeList.Add(interfaceType);
            }
            typeList.Add(typeof(T)/*objectType*/);

            var map = new Dictionary<string, Tuple<PropertyInfo, int>>();

            var index = 0;
            foreach (Type type in typeList)
            {
                foreach (var property in type.GetProperties())
                {
                    map[property.Name] = Tuple.Create(property, index);
                    index++;
                }
            }

            return map;
        }

        internal static IDictionary<PropertyInfo, ReflectedProperty> GetPropertyMap(Type objectType)
        {
            //var type = !objectType.IsInterface ? Reflector.ExtractInterface(objectType) : objectType;
            var methodHandle = _getPropertyMapCache.GetOrAdd(objectType, t => _getPropertyMapMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (IDictionary<PropertyInfo, ReflectedProperty>)func(null, new object[] { });
        }

        internal static IDictionary<PropertyInfo, ReflectedProperty> GetPropertyMap<T>()
        {
            return PropertyCache<T>.Map;
        }

        internal static IDictionary<string, ReflectedProperty> GetPropertyNameMap(Type objectType)
        {
            var methodHandle = _getPropertyNameMapCache.GetOrAdd(objectType, t => _getPropertyNameMapMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (IDictionary<string, ReflectedProperty>)func(null, new object[] { });
        }

        internal static IDictionary<string, ReflectedProperty> GetPropertyNameMap<T>()
        {
            return PropertyCache<T>.NameMap;
        }

        internal static ReflectedType GetReflectedType(Type objectType)
        {
            var methodHandle = _getReflectedTypeCache.GetOrAdd(objectType, t => _getReflectedTypeMethod.MakeGenericMethod(t).MethodHandle);
            var func = Reflector.Method.CreateDelegate(methodHandle);
            return (ReflectedType)func(null, new object[] { });
        }

        internal static ReflectedType GetReflectedType<T>()
        {
            return TypeCache<T>.Type;
        }

        public static Type ExtractCollectionElementType(Type collectionType)
        {
            var elementType = collectionType.GetElementType();
            if (elementType == null)
            {
                elementType = ExtractGenericCollectionElementType(collectionType);
            }
            return elementType;
        }

        public static Type ExtractGenericCollectionElementType(Type genericCollectionType)
        {
            Type elementType = null;
            var genericArguments = genericCollectionType.GetGenericArguments();
            if (genericArguments != null && genericArguments.Length == 1)
            {
                elementType = genericArguments[0];
            }
            return elementType;
        }

        public static RuntimeMethodHandle GetDefaultConstructor(Type objectType)
        {
            var ctor = _defaultConstructors.GetOrAdd(objectType, type => type.GetConstructor(Type.EmptyTypes).MethodHandle);
            return ctor;
        }

        public static IEnumerable<Type> AllTypes()
        {
            var appDomain = AppDomain.CurrentDomain;
            if (appDomain != null)
            {
                var assemblies = appDomain.GetAssemblies();
                foreach (var asm in assemblies)
                {
                    Type[] types = null;
                    try
                    {
                        types = asm.GetTypes();
                    }
                    catch { }

                    if (types != null)
                    {
                        foreach (var type in types)
                        {
                            yield return type;
                        }
                    }
                }
            }
        }

        public static IEnumerable<Type> AllBusinessObjectTypes()
        {
            return Reflector.AllTypes().Where(t => Reflector.IsBusinessObject(t));
        }

        public static IEnumerable<MethodInfo> GetExtensionMethods(this Type extendedType)
        {
            var methods = extendedType.Assembly.GetTypes().Where(t => t.IsSealed && !t.IsGenericType && !t.IsNested).SelectMany(t => t.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)).
                        Where(m => m.IsDefined(typeof(ExtensionAttribute), false) && m.GetParameters()[0].ParameterType == extendedType);
            return methods;
        }

        public static IList<A> GetAttributeList<T, A>()
            where A : Attribute
        {
            return ClassAttributeListCache<T, A>.AttributeList;
        }

        public static A GetAttribute<T, A>(bool assemblyLevel = false, bool inherit = false)
            where A :Attribute
        {
            A attribute = null;
            if (!typeof(T).IsValueType)
            {
                attribute = assemblyLevel ? typeof(T).Assembly.GetCustomAttributes(typeof(T), inherit).OfType<A>().FirstOrDefault() : (inherit ? ClassHierarchyAttributeCache<T, A>.Attribute : ClassAttributeCache<T, A>.Attribute);
            }
            return attribute;
        }

        public static T GetAttribute<T>(Type objectType, bool assemblyLevel = false, bool inherit = false)
            where T : Attribute
        {
            T attribute = null;
            if (objectType != null && !objectType.IsValueType)
            {
                object[] attributes = assemblyLevel ? objectType.Assembly.GetCustomAttributes(typeof(T), inherit) : objectType.GetCustomAttributes(typeof(T), inherit);
                attribute = attributes.Cast<T>().FirstOrDefault();
            }
            return attribute;
        }

        public static Type GetType(string typeName)
        {
            return Type.GetType(typeName, false, true);
        }
                
        #region CLR to DB Type

        private static readonly Dictionary<Type, DbType> _clrToDbTypeLookup = new Dictionary<Type, DbType>
        {
            {typeof(bool),DbType.Boolean},
            {typeof(bool?),DbType.Boolean},
            {typeof(byte),DbType.Byte},
            {typeof(byte?),DbType.Byte},
            {typeof(short),DbType.UInt16},
            {typeof(short?),DbType.UInt16},
            {typeof(int),DbType.Int32},
            {typeof(int?),DbType.Int32},
            {typeof(long),DbType.Int64},
            {typeof(long?),DbType.Int64},
            {typeof(sbyte),DbType.SByte},
            {typeof(sbyte?),DbType.SByte},
            {typeof(uint),DbType.UInt32},
            {typeof(uint?),DbType.UInt32},
            {typeof(ushort),DbType.UInt16},
            {typeof(ushort?),DbType.UInt16},
            {typeof(ulong),DbType.UInt64},
            {typeof(ulong?),DbType.UInt64},
            {typeof(decimal),DbType.Decimal},
            {typeof(decimal?),DbType.Decimal},
            {typeof(float),DbType.Single},
            {typeof(float?),DbType.Single},
            {typeof(double),DbType.Double},
            {typeof(double?),DbType.Double},
            {typeof(char),DbType.String},
            {typeof(char?),DbType.String},
            {typeof(string),DbType.String},
            {typeof(DateTime),DbType.DateTime},
            {typeof(DateTime?),DbType.DateTime},
            {typeof(DateTimeOffset),DbType.DateTimeOffset},
            {typeof(DateTimeOffset?),DbType.DateTimeOffset},
            {typeof(TimeSpan),DbType.Time},
            {typeof(TimeSpan?),DbType.Time},
            {typeof(byte[]),DbType.Binary},
            {typeof(Guid),DbType.Guid},           
            {typeof(Guid?),DbType.Guid},           
            {typeof(object),DbType.Object}
        };

        internal static DbType ClrToDbType(Type clrType)
        {
            DbType dbType;
            if (clrType == null)
            {
                dbType = DbType.Xml;
            }
            else if (!_clrToDbTypeLookup.TryGetValue(clrType, out dbType))
            {
                if (clrType.IsEnum)
                {
                    dbType = DbType.Int32;
                }
                else
                {
                    dbType = DbType.Xml;
                } 
            }
            return dbType;
        }

        #endregion

        #region CLR to XML Schema Type

        private static Dictionary<Type, string> _clrToXmlLookup = new Dictionary<Type, string>
        {
            {typeof(Uri),"xs:anyURI"},
            {typeof(byte),"xs:unsignedByte"},
            {typeof(sbyte),"xs:byte"},
            {typeof(byte[]), "xs:base64Binary"},
            {typeof(bool),"xs:boolean"},
            {typeof(DateTimeOffset),"xs:dateTime"},
            {typeof(DateTime),"xs:dateTime"},
            {typeof(TimeSpan),"xs:diration"},
            {typeof(decimal),"xs:decimal"},
            {typeof(double),"xs:double"},
            {typeof(float),"xs:float"},
            {typeof(short),"xs:short"},
            {typeof(int),"xs:int"},
            {typeof(long),"xs:long"},
            {typeof(ushort),"xs:unsignedShort"},
            {typeof(uint), "xs:unsignedInt"},
            {typeof(ulong),"xs:unsignedLong"},
            {typeof(string), "xs:string"},
            {typeof(char), "xs:string"},
            {typeof(Guid), "xs:string"}
        };

        internal static string SimpleClrToXmlType(Type clrType)
        {
            var schemaType = string.Empty;
            if (Reflector.IsNullableType(clrType))
            {
                return SimpleClrToXmlType(clrType.GetGenericArguments()[0]);
            }
            else if (!_clrToXmlLookup.TryGetValue(clrType, out schemaType))
            {
                schemaType = string.Empty;
            }
            return schemaType;
        }

        internal static string ClrToXmlType(Type clrType)
        {
            var reflectedType = GetReflectedType(clrType);
            
            var schemaType = string.Empty;
            if (reflectedType.IsNullableType)
            {
                return ClrToXmlType(clrType.GetGenericArguments()[0]);
            }
            else if (clrType == typeof(string) || clrType.IsValueType)
            {
                if (!_clrToXmlLookup.TryGetValue(clrType, out schemaType))
                {
                    schemaType = string.Empty;
                }
            }
            else if (reflectedType.IsSimpleList)
            {
                schemaType = "xs:anyType";
            }
            else if (reflectedType.IsBusinessObject)
            {
                schemaType = clrType.Name;
                if (Reflector.IsEmitted(clrType))
                {
                    schemaType = Reflector.ExtractInterface(clrType).Name; 
                }
            }
            else if (reflectedType.IsList)
            {
                var elementType = reflectedType.ElementType;
                if (Reflector.IsEmitted(elementType))
                {
                    elementType = Reflector.ExtractInterface(elementType);
                }
                schemaType = "ArrayOf" + elementType.Name;
            }
            else if (reflectedType.IsTypeUnion)
            {
                schemaType = "TypeUnionOf_" + reflectedType.GenericArguments.Select(t => XmlConvert.EncodeName(t.Name)).ToDelimitedString("_");
            }
            return schemaType;
        }

        public static ObjectTypeCode GetObjectTypeCode(Type type)
        {
            var typeCode = Type.GetTypeCode(type);
            switch(typeCode)
            {
                case TypeCode.Boolean:
                    return ObjectTypeCode.Boolean;
                case TypeCode.Byte:
                    return ObjectTypeCode.Byte;
                case TypeCode.UInt16:
                    return ObjectTypeCode.UInt16;
                case TypeCode.UInt32:
                    return ObjectTypeCode.UInt32;
                case TypeCode.UInt64:
                    return ObjectTypeCode.UInt64;
                case TypeCode.SByte:
                    return ObjectTypeCode.SByte;
                case TypeCode.Int16: 
                    return ObjectTypeCode.Int16;
                case TypeCode.Int32:
                    return ObjectTypeCode.Int32;
                case TypeCode.Int64:
                    return ObjectTypeCode.Int64;
                case TypeCode.Char:
                    return ObjectTypeCode.Char;
                case TypeCode.String:
                    return ObjectTypeCode.String;
                case TypeCode.Single:
                    return ObjectTypeCode.Single;
                case TypeCode.Double:
                    return ObjectTypeCode.Double;
                case TypeCode.Decimal:
                    return ObjectTypeCode.Decimal;
                case TypeCode.DateTime: 
                    return ObjectTypeCode.DateTime;
                case TypeCode.DBNull:
                    return ObjectTypeCode.DBNull;
                default:
                    var name = type.Name;
                    if (Reflector.IsBusinessObject(type))
                    {
                        return ObjectTypeCode.BusinessObject;
                    }
                    else if (Reflector.IsList(type))
                    {
                        var elementType = Reflector.ExtractCollectionElementType(type);
                        if (Reflector.IsBusinessObject(elementType))
                        {
                            return ObjectTypeCode.BusinessObjectList;
                        }
                        else
                        {
                            return ObjectTypeCode.ObjectList;
                        }
                    }
                    else if (Reflector.IsTypeUnion(type))
                    {
                        return ObjectTypeCode.TypeUnion;
                    }
                    else if (name == "Byte[]")
                    {
                        return ObjectTypeCode.ByteArray;
                    }
                    else if (name == "Char[]")
                    {
                        return ObjectTypeCode.CharArray;
                    }
                    else if (name == "TimeSpan")
                    {
                        return ObjectTypeCode.TimeSpan;
                    }
                    else if (name == "Guid")
                    {
                        return  ObjectTypeCode.Guid;
                    }
                    else if (name == "DateTimeOffset")
                    {
                        return  ObjectTypeCode.DateTimeOffset;
                    }
                    else if (name == "Version")
                    {
                        return  ObjectTypeCode.Version;
                    }
                    else if (name == "Uri")
                    {
                        return ObjectTypeCode.Uri;
                    }
                    else if (Reflector.IsDictionary(type))
                    {
                        return ObjectTypeCode.ObjectMap;
                    }
                    else
                    {
                        return ObjectTypeCode.Object;
                    }
            }
        }

        #endregion
        
        #region Emit Extensions

        internal static void PushInstance(this ILGenerator il, Type type)
        {
            il.Emit(OpCodes.Ldarg_0);
            if (type.IsValueType)
            {
                il.Emit(OpCodes.Unbox, type);
            }
        }

        internal static void BoxIfNeeded(this ILGenerator il, Type type)
        {
            if (type.IsValueType)
            {
                il.Emit(OpCodes.Box, type);
            }
        }

        internal static void UnboxIfNeeded(this ILGenerator il, Type type)
        {
            if (type.IsValueType)
            {
                il.Emit(OpCodes.Unbox_Any, type);
            }
        }

        internal static void EmitCastToReference(this ILGenerator il, System.Type type)
        {
            if (type.IsValueType)
            {
                il.Emit(OpCodes.Unbox_Any, type);
            }
            else
            {
                il.Emit(OpCodes.Castclass, type);
            }
        }

        internal static void EmitFastInt(this ILGenerator il, int value)
        {
            switch (value)
            {
                case -1:
                    il.Emit(OpCodes.Ldc_I4_M1);
                    return;
                case 0:
                    il.Emit(OpCodes.Ldc_I4_0);
                    return;
                case 1:
                    il.Emit(OpCodes.Ldc_I4_1);
                    return;
                case 2:
                    il.Emit(OpCodes.Ldc_I4_2);
                    return;
                case 3:
                    il.Emit(OpCodes.Ldc_I4_3);
                    return;
                case 4:
                    il.Emit(OpCodes.Ldc_I4_4);
                    return;
                case 5:
                    il.Emit(OpCodes.Ldc_I4_5);
                    return;
                case 6:
                    il.Emit(OpCodes.Ldc_I4_6);
                    return;
                case 7:
                    il.Emit(OpCodes.Ldc_I4_7);
                    return;
                case 8:
                    il.Emit(OpCodes.Ldc_I4_8);
                    return;
            }

            if (value > -129 && value < 128)
            {
                il.Emit(OpCodes.Ldc_I4_S, (SByte)value);
            }
            else
            {
                il.Emit(OpCodes.Ldc_I4, value);
            }
        }

        #endregion

        internal static class InterfaceCache<T>
        {
            static InterfaceCache()
            {
                Interfaces = Reflector.CacheInterfaces<T>();
            }
            public static readonly Dictionary<Type, List<Type>> Interfaces;
        }

        internal static class PropertyCache<T>
        {
            static PropertyCache()
            {
                var cachedProperties = Reflector.CacheProperties<T>();
                Properties = new ReadOnlyDictionary<string, PropertyInfo>(cachedProperties.ToDictionary(p => p.Key, p => p.Value.Item1));
                Positions = new ReadOnlyDictionary<string, int>(cachedProperties.ToDictionary(p => p.Key, p => p.Value.Item2));

                var map = MappingFactory.GetEntityMap(typeof(T));
                if (map != null)
                {
                    var reflectedProperties = map.Properties.ToDictionary(p => p.Property.PropertyName, p => p.Property);
                    Map = new ReadOnlyDictionary<PropertyInfo, ReflectedProperty>(Properties.Values.ToDictionary(p => p, p =>
                    {
                        ReflectedProperty r;
                        if (!reflectedProperties.TryGetValue(p.Name, out r))
                        {
                            r = new ReflectedProperty(p, cachedProperties[p.Name].Item2);
                        }
                        return r;
                    }));
                }
                else
                {
                    Map = new ReadOnlyDictionary<PropertyInfo, ReflectedProperty>(Properties.Values.ToDictionary(p => p, p => new ReflectedProperty(p, cachedProperties[p.Name].Item2)));
                } 
                
                NameMap = new ReadOnlyDictionary<string, ReflectedProperty>(Map.ToDictionary(p => p.Key.Name, p => p.Value));
            }
            public static readonly ReadOnlyDictionary<string, PropertyInfo> Properties;
            internal static readonly ReadOnlyDictionary<PropertyInfo, ReflectedProperty> Map;
            internal static readonly ReadOnlyDictionary<string, ReflectedProperty> NameMap;
            internal static readonly ReadOnlyDictionary<string, int> Positions;
        }

        internal static class ClassAttributeCache<T, A>
            where A : Attribute
        {
            static ClassAttributeCache()
            {
                Attribute = (A)typeof(T).GetCustomAttributes(typeof(A), false).FirstOrDefault();
            }

            public static readonly A Attribute;
        }

        internal static class ClassHierarchyAttributeCache<T, A>
            where A : Attribute
        {
            static ClassHierarchyAttributeCache()
            {
                Attribute = (A)typeof(T).GetCustomAttributes(typeof(A), true).FirstOrDefault();
            }

            public static readonly A Attribute;
        }

        internal static class ClassAttributeListCache<T, A>
            where A : Attribute
        {
            static ClassAttributeListCache()
            {
                AttributeList = typeof(T).GetCustomAttributes(typeof(A), true).Cast<A>().ToList();
            }

            public static readonly IList<A> AttributeList;
        }

        internal class TypeCache<T>
        {
            static TypeCache()
            {
                Type = new ReflectedType(typeof(T));
                Type.XmlElementName = Xml.GetElementNameFromType(typeof(T));
            }

            public readonly static ReflectedType Type;
        }
    }
}
