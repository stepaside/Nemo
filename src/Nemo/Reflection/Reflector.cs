using Nemo.Collections.Extensions;
using Nemo.Configuration.Mapping;
using Nemo.Serialization;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Xml.Linq;

namespace Nemo.Reflection
{
    public static partial class Reflector
    {
        #region Declarations

        private static readonly MethodInfo GetReflectedTypeMethod = typeof(Reflector).GetMethod("GetReflectedType", BindingFlags.Static | BindingFlags.Public, null, Type.EmptyTypes, null);
        private static readonly MethodInfo GetPropertyMapMethod = typeof(Reflector).GetMethod("GetPropertyMap", BindingFlags.Static | BindingFlags.Public, null, Type.EmptyTypes, null);
        private static readonly MethodInfo GetPropertyNameMapMethod = typeof(Reflector).GetMethod("GetPropertyNameMap", BindingFlags.Static | BindingFlags.Public, null, Type.EmptyTypes, null);
        private static readonly MethodInfo GetAllPropertiesMethod = typeof(Reflector).GetMethod("GetAllProperties", Type.EmptyTypes);
        private static readonly MethodInfo GetAllPropertyPositionsMethod = typeof(Reflector).GetMethod("GetAllPropertyPositions", Type.EmptyTypes);
        private static readonly MethodInfo GetPropertyMethod = typeof(Reflector).GetMethod("GetProperty", new[] { typeof(string) });
        private static readonly MethodInfo GetInterfaceMethod = typeof(Reflector).GetMethod("GetInterface", Type.EmptyTypes);
        private static readonly MethodInfo GetInterfacesMethod = typeof(Reflector).GetMethod("GetIntefaces", Type.EmptyTypes);

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetReflectedTypeCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetPropertyMapCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetPropertyNameMapCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetAllPropertiesCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetAllPropertyPositionsCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetPropertyCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> ExtractInterfaceCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> ExtractInterfacesCache = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> DefaultConstructors = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        
        #endregion

        public static bool InheritsFrom(this Type thisType, Type baseType)
        {
            return baseType.IsAssignableFrom(thisType);
        }

        public static object GetDefault(this Type type)
        {
            return type.IsValueType ? System.Activator.CreateInstance(type) : null;
        }

        public static string GetFriendlyName(this Type type)
        {
            if (type.IsGenericType)
            {
                return type.Name.Split('`')[0] + "<" + string.Join(", ", type.GetGenericArguments().Select(x => GetFriendlyName(x)).ToArray()) + ">";
            }
            return type.Name;
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

        public static bool IsDataEntity(object objectValue)
        {
            return objectValue is IDataEntity;
        }

        public static bool IsDataEntity(Type objectType)
        {
            return typeof(IDataEntity).IsAssignableFrom(objectType);
        }

        public static bool IsDataEntityList(Type objectType, out Type elementType)
        {
            var result = false;
            if (IsList(objectType))
            {
                elementType = GetElementType(objectType);
                if (elementType != null && (IsDataEntity(elementType) || !IsSimpleType(elementType)))
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
            return objectType == typeof(IDataEntity) || objectType == typeof(ITrackableDataEntity);
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
            return IsNullableType(objectType, out var dummy);
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
                return true;
            }
            return objectType.GetInterface("System.Collections.IList") != null;
        }

        public static bool IsSimpleList(Type type)
        {
            bool result = false;
            if (IsList(type))
            {
                result = IsSimpleType(GetElementType(type));
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
                return true;
            }
            return objectType.GetInterface("System.Collections.IDictionary") != null;
        }

        public static bool IsArray(Type objectType)
        {
            return objectType.IsArray;
        }

        public static bool IsTuple(Type objectType)
        {
#if !NETSTANDARD2_0
            return objectType.GetInterface(typeof(ITuple).Name) != null;
#else
            if (!objectType.IsGenericType)
            {
                return false;
            }

            var genericTypeDefinition = objectType.GetGenericTypeDefinition();
            return genericTypeDefinition == typeof(Tuple<>)
                || genericTypeDefinition == typeof(Tuple<,>)
                || genericTypeDefinition == typeof(Tuple<,,>)
                || genericTypeDefinition == typeof(Tuple<,,,>)
                || genericTypeDefinition == typeof(Tuple<,,,,>)
                || genericTypeDefinition == typeof(Tuple<,,,,,>)
                || genericTypeDefinition == typeof(Tuple<,,,,,,>)
                || genericTypeDefinition == typeof(Tuple<,,,,,,,>);
#endif
        }

        public static IList<Type> GetTupleTypes(Type objectType)
        {
            return objectType.GetGenericArguments().SelectMany(t => Reflector.IsTuple(t) ? GetTupleTypes(t) : new[] { t }).ToList();
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

        public static Type GetInterface(Type objectType)
        {
            var methodHandle = ExtractInterfaceCache.GetOrAdd(objectType, t => GetInterfaceMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (Type)func(null, new object[] { });
        }

        public static Type GetInterface<T>()
        {
            var interfaces = InterfaceCache<T>.Interfaces;
            var interfaceType = interfaces.MaxElement(k => k.Value.Count).Key;
            return interfaceType;
        }

        public static IEnumerable<Type> GetInterfaces(Type objectType)
        {
            var methodHandle = ExtractInterfacesCache.GetOrAdd(objectType, t => GetInterfacesMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (IEnumerable<Type>)func(null, new object[] { });
        }

        public static IEnumerable<Type> GetInterfaces<T>()
        {
            var interfaces = InterfaceCache<T>.Interfaces;
            return interfaces.Keys;
        }

        private static Dictionary<Type, List<Type>> PrepareInterfaces<T>()
        {
            var interfaceTypes = GetInterfacesImplementation(typeof(T));
            var interfaceMap = new Dictionary<Type, List<Type>>();

            foreach (var interfaceType in interfaceTypes)
            {
                var interfaces = GetInterfacesImplementation(interfaceType).ToList();
                if (interfaces.Count > 0)
                {
                    interfaceMap[interfaceType] = interfaces;
                }
            }
            return interfaceMap;
        }

        private static IEnumerable<Type> GetInterfacesImplementation(Type type)
        {
            var interfaces = type.GetInterfaces();
            Array.Reverse(interfaces);
            return (type.IsInterface ? interfaces.Append(type) : interfaces).Where(t => t != typeof(IDataEntity) && typeof(IDataEntity).IsAssignableFrom(t));
        }

        public static PropertyInfo[] GetAllProperties(Type objectType)
        {
            var methodHandle = GetAllPropertiesCache.GetOrAdd(objectType, t => GetAllPropertiesMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (PropertyInfo[])func(null, new object[] { });
        }

        public static PropertyInfo[] GetAllProperties<T>()
        {
            var properties = PropertyCache<T>.Properties;
            return properties.Values.ToArray();
        }

        public static PropertyInfo GetProperty(Type objectType, string name)
        {
            var methodHandle = GetPropertyCache.GetOrAdd(objectType, t => GetPropertyMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (PropertyInfo)func(null, new object[] { name });
        }

        public static PropertyInfo GetProperty<T>(string name)
        {
           var properties = PropertyCache<T>.Properties;
            PropertyInfo property;
            properties.TryGetValue(name, out property);
            return property;
        }

        public static IDictionary<string, int> GetAllPropertyPositions(Type objectType)
        {
            var methodHandle = GetAllPropertyPositionsCache.GetOrAdd(objectType, t => GetAllPropertyPositionsMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (IDictionary<string, int>)func(null, new object[] { });
        }

        public static IDictionary<string, int> GetAllPropertyPositions<T>()
        {
            return PropertyCache<T>.Positions;
        }

        private static Dictionary<string, Tuple<PropertyInfo, int>> PrepareAllProperties<T>()
        {
            var typeList = new HashSet<Type>();
            foreach (var interfaceType in GetInterfaces<T>(/*objectType*/))
            {
                typeList.Add(interfaceType);
            }
            typeList.Add(typeof(T)/*objectType*/);

            var map = new Dictionary<string, Tuple<PropertyInfo, int>>();

            var index = 0;
            foreach (var type in typeList)
            {
                foreach (var property in type.GetProperties())
                {
                    map[property.Name] = Tuple.Create(property, index);
                    index++;
                }
            }

            return map;
        }

        public static IDictionary<PropertyInfo, ReflectedProperty> GetPropertyMap(Type objectType)
        {
            //var type = !objectType.IsInterface ? Reflector.GetInterface(objectType) : objectType;
            var methodHandle = GetPropertyMapCache.GetOrAdd(objectType, t => GetPropertyMapMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (IDictionary<PropertyInfo, ReflectedProperty>)func(null, new object[] { });
        }

        public static IDictionary<PropertyInfo, ReflectedProperty> GetPropertyMap<T>()
        {
            return PropertyCache<T>.Map;
        }

        public static IDictionary<string, ReflectedProperty> GetPropertyNameMap(Type objectType)
        {
            var methodHandle = GetPropertyNameMapCache.GetOrAdd(objectType, t => GetPropertyNameMapMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (IDictionary<string, ReflectedProperty>)func(null, new object[] { });
        }

        public static IDictionary<string, ReflectedProperty> GetPropertyNameMap<T>()
        {
            return PropertyCache<T>.NameMap;
        }

        public static ReflectedType GetReflectedType(Type objectType)
        {
            var methodHandle = GetReflectedTypeCache.GetOrAdd(objectType, t => GetReflectedTypeMethod.MakeGenericMethod(t).MethodHandle);
            var func = Method.CreateDelegate(methodHandle);
            return (ReflectedType)func(null, new object[] { });
        }

        public static ReflectedType GetReflectedType<T>()
        {
            return TypeCache<T>.Type;
        }
        
        public static RuntimeMethodHandle GetDefaultConstructor(Type objectType)
        {
            var ctor = DefaultConstructors.GetOrAdd(objectType, type => type.GetConstructor(Type.EmptyTypes).MethodHandle);
            return ctor;
        }

        public static IEnumerable<Type> AllTypes()
        {
            var appDomain = AppDomain.CurrentDomain;
            var assemblies = appDomain.GetAssemblies();
            foreach (var asm in assemblies)
            {
                Type[] types = null;
                try
                {
                    types = asm.GetTypes();
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch
                {
                }

                if (types == null) continue;
                foreach (var type in types)
                {
                    yield return type;
                }
            }
        }

        public static IEnumerable<Type> AllDataEntityTypes()
        {
            return AllTypes().Where(IsDataEntity);
        }

        public static IEnumerable<MethodInfo> GetExtensionMethods(this Type extendedType)
        {
            var methods = extendedType.Assembly.GetTypes()
                .Where(t => t.IsSealed && !t.IsGenericType && !t.IsNested)
                .SelectMany(t => t.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
                .Where(m => m.IsDefined(typeof(ExtensionAttribute), false) && m.GetParameters()[0].ParameterType == extendedType);
            return methods;
        }

        public static IList<TAttribute> GetAttributeList<T, TAttribute>()
            where TAttribute : Attribute
        {
            return ClassAttributeListCache<T, TAttribute>.AttributeList;
        }

        public static TAttribute GetAttribute<T, TAttribute>(bool assemblyLevel = false, bool inherit = false)
            where TAttribute : Attribute
        {
            TAttribute attribute = null;
            if (!typeof(T).IsValueType)
            {
                attribute = assemblyLevel ? typeof(T).Assembly.GetCustomAttributes(typeof(T), inherit).OfType<TAttribute>().FirstOrDefault() : (inherit ? ClassHierarchyAttributeCache<T, TAttribute>.Attribute : ClassAttributeCache<T, TAttribute>.Attribute);
            }
            return attribute;
        }

        public static T GetAttribute<T>(Type objectType, bool assemblyLevel = false, bool inherit = false)
            where T : Attribute
        {
            if (objectType == null || objectType.IsValueType) return null;
            var attributes = assemblyLevel ? objectType.Assembly.GetCustomAttributes(typeof(T), inherit) : objectType.GetCustomAttributes(typeof(T), inherit);
            var attribute = attributes.Cast<T>().FirstOrDefault();
            return attribute;
        }

        public static Type GetType(string typeName)
        {
            return Type.GetType(typeName, false, true);
        }

        public static Type GetRecusrsiveElementType(Type collectionType)
        {
            var current = GetElementType(collectionType);
            var elementType = current;
            while (current != null)
            {
                elementType = current;
                current = GetElementType(elementType);
            }
            return elementType;
        }

        public static Type GetElementType(Type collectionType)
        {
            var ienum = FindEnumerable(collectionType);
            return ienum != null ? ienum.GetGenericArguments()[0] : null;
        }

        private static Type FindEnumerable(Type collectionType)
        {
            if (collectionType == null || collectionType == typeof(string))
            {
                return null;
            }

            if (collectionType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(collectionType.GetElementType());
            }

            if (collectionType.IsGenericType)
            {
                foreach (var arg in collectionType.GetGenericArguments())
                {
                    var ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(collectionType))
                    {
                        return ienum;
                    }
                }
            }

            var interfaces = collectionType.GetInterfaces();
            if (interfaces.Length > 0)
            {
                foreach (var iface in interfaces)
                {
                    var ienum = FindEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }

            if (collectionType.BaseType != null && collectionType.BaseType != typeof(object))
            {
                return FindEnumerable(collectionType.BaseType);
            }

            return null;
        }

        /// <summary>
        /// Retrieves a value from  a static property by specifying a type full name and property
        /// </summary>
        /// <param name="typeName">Full type name (namespace.class)</param>
        /// <param name="property">Property to get value from</param>
        /// <returns></returns>
        public static object GetStaticProperty(string typeName, string property)
        {
            Type type = GetTypeFromName(typeName);
            if (type == null)
                return null;

            return GetStaticProperty(type, property);
        }

        /// <summary>
        /// Returns a static property from a given type
        /// </summary>
        /// <param name="type">Type instance for the static property</param>
        /// <param name="property">Property name as a string</param>
        /// <returns></returns>
        public static object GetStaticProperty(Type type, string property)
        {
            object result = null;
            try
            {
                result = type.InvokeMember(property, BindingFlags.Static | BindingFlags.Public | BindingFlags.GetField | BindingFlags.GetProperty, null, type, null);
            }
            catch
            {
                return null;
            }

            return result;
        }

        /// <summary>
        /// Helper routine that looks up a type name and tries to retrieve the
        /// full type reference using GetType() and if not found looking 
        /// in the actively executing assemblies and optionally loading
        /// the specified assembly name.
        /// </summary>
        /// <param name="typeName">type to load</param>
        /// <param name="assemblyName">
        /// Optional assembly name to load from if type cannot be loaded initially. 
        /// Use for lazy loading of assemblies without taking a type dependency.
        /// </param>
        /// <returns>null</returns>
        public static Type GetTypeFromName(string typeName, string assemblyName)
        {
            var type = Type.GetType(typeName, false);
            if (type != null)
                return type;

            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            // try to find manually
            foreach (Assembly asm in assemblies)
            {
                type = asm.GetType(typeName, false);

                if (type != null)
                    break;
            }
            if (type != null)
                return type;

            // see if we can load the assembly
            if (!string.IsNullOrEmpty(assemblyName))
            {
                var a = LoadAssembly(assemblyName);
                if (a != null)
                {
                    type = Type.GetType(typeName, false);
                    if (type != null)
                        return type;
                }
            }

            return null;
        }

        /// <summary>
        /// Overload for backwards compatibility which only tries to load
        /// assemblies that are already loaded in memory.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>        
        public static Type GetTypeFromName(string typeName)
        {
            return GetTypeFromName(typeName, null);
        }

        /// <summary>
        /// Try to load an assembly into the application's app domain.
        /// Loads by name first then checks for filename
        /// </summary>
        /// <param name="assemblyName">Assembly name or full path</param>
        /// <returns>null on failure</returns>
        public static Assembly LoadAssembly(string assemblyName)
        {
            Assembly assembly = null;
            try
            {
                assembly = Assembly.Load(assemblyName);
            }
            catch { }

            if (assembly != null)
                return assembly;

            if (File.Exists(assemblyName))
            {
                assembly = Assembly.LoadFrom(assemblyName);
                if (assembly != null)
                    return assembly;
            }
            return null;
        }

        #region CLR to DB Type

        private static readonly Dictionary<Type, DbType> ClrToDbTypeLookup = new Dictionary<Type, DbType>
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
            else if (!ClrToDbTypeLookup.TryGetValue(clrType, out dbType))
            {
                dbType = clrType.IsEnum ? DbType.Int32 : DbType.Xml; 
            }
            return dbType;
        }

        #endregion

        #region CLR to XML Schema Type

        private static readonly Dictionary<Type, string> ClrToXmlLookup = new Dictionary<Type, string>
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
            while (true)
            {
                string schemaType;
                if (IsNullableType(clrType))
                {
                    clrType = clrType.GetGenericArguments()[0];
                    continue;
                }
                if (!ClrToXmlLookup.TryGetValue(clrType, out schemaType))
                {
                    schemaType = string.Empty;
                }
                return schemaType;
            }
        }

        internal static string ClrToXmlType(Type clrType)
        {
            while (true)
            {
                var reflectedType = GetReflectedType(clrType);

                var schemaType = string.Empty;
                if (reflectedType.IsNullableType)
                {
                    clrType = clrType.GetGenericArguments()[0];
                    continue;
                }
                if (clrType == typeof(string) || clrType.IsValueType)
                {
                    if (!ClrToXmlLookup.TryGetValue(clrType, out schemaType))
                    {
                        schemaType = string.Empty;
                    }
                }
                else if (reflectedType.IsSimpleList)
                {
                    schemaType = "xs:anyType";
                }
                else if (reflectedType.IsDataEntity)
                {
                    schemaType = clrType.Name;
                    if (IsEmitted(clrType))
                    {
                        schemaType = GetInterface(clrType).Name;
                    }
                }
                else if (reflectedType.IsList)
                {
                    var elementType = reflectedType.ElementType;
                    if (IsEmitted(elementType))
                    {
                        elementType = GetInterface(elementType);
                    }
                    schemaType = "ArrayOf" + elementType.Name;
                }
                return schemaType;
            }
        }

        public static ObjectTypeCode GetObjectTypeCode(Type type)
        {
            var typeCode = Type.GetTypeCode(type);
            switch (typeCode)
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

                    if (IsDataEntity(type))
                    {
                        return ObjectTypeCode.Object;
                    }

                    if (IsList(type))
                    {
                        var elementType = GetElementType(type);
                        var isEntityList = IsDataEntity(elementType) || !IsSimpleType(elementType);
                        if (isEntityList && elementType.IsAbstract && !elementType.IsInterface)
                        {
                            return ObjectTypeCode.PolymorphicObjectList;
                        }
                        return isEntityList ? ObjectTypeCode.ObjectList : ObjectTypeCode.SimpleList;
                    }

                    if (name == "Byte[]")
                    {
                        return ObjectTypeCode.ByteArray;
                    }

                    if (name == "Char[]")
                    {
                        return ObjectTypeCode.CharArray;
                    }

                    if (name == "TimeSpan")
                    {
                        return ObjectTypeCode.TimeSpan;
                    }

                    if (name == "Guid")
                    {
                        return ObjectTypeCode.Guid;
                    }

                    if (name == "DateTimeOffset")
                    {
                        return ObjectTypeCode.DateTimeOffset;
                    }

                    if (name == "Version")
                    {
                        return ObjectTypeCode.Version;
                    }

                    if (name == "Uri")
                    {
                        return ObjectTypeCode.Uri;
                    }

                    return IsDictionary(type) ? ObjectTypeCode.ObjectMap : ObjectTypeCode.Object;
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

        internal static void EmitCastToReference(this ILGenerator il, Type type)
        {
            il.Emit(type.IsValueType ? OpCodes.Unbox_Any : OpCodes.Castclass, type);
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
                Interfaces = PrepareInterfaces<T>();
            }
            // ReSharper disable once StaticMemberInGenericType
            internal static readonly Dictionary<Type, List<Type>> Interfaces;
        }

        internal static class PropertyCache<T>
        {
            static PropertyCache()
            {
                var cachedProperties = PrepareAllProperties<T>();
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
            // ReSharper disable once StaticMemberInGenericType
            internal static readonly ReadOnlyDictionary<string, PropertyInfo> Properties;
            // ReSharper disable once StaticMemberInGenericType
            internal static readonly ReadOnlyDictionary<PropertyInfo, ReflectedProperty> Map;
            // ReSharper disable once StaticMemberInGenericType
            internal static readonly ReadOnlyDictionary<string, ReflectedProperty> NameMap;
            // ReSharper disable once StaticMemberInGenericType
            internal static readonly ReadOnlyDictionary<string, int> Positions;
        }

        internal static class ClassAttributeCache<T, TAttribute>
            where TAttribute : Attribute
        {
            static ClassAttributeCache()
            {
                Attribute = (TAttribute)typeof(T).GetCustomAttributes(typeof(TAttribute), false).FirstOrDefault();
            }

            internal static readonly TAttribute Attribute;
        }

        internal static class ClassHierarchyAttributeCache<T, TAttribute>
            where TAttribute : Attribute
        {
            static ClassHierarchyAttributeCache()
            {
                Attribute = (TAttribute)typeof(T).GetCustomAttributes(typeof(TAttribute), true).FirstOrDefault();
            }

            internal static readonly TAttribute Attribute;
        }

        internal static class ClassAttributeListCache<T, TAttribute>
            where TAttribute : Attribute
        {
            static ClassAttributeListCache()
            {
                AttributeList = typeof(T).GetCustomAttributes(typeof(TAttribute), true).Cast<TAttribute>().ToList();
            }

            internal static readonly IList<TAttribute> AttributeList;
        }

        internal class TypeCache<T>
        {
            static TypeCache()
            {
                Type = new ReflectedType(typeof(T));
                Type.XmlElementName = Xml.GetElementNameFromType<T>();
            }

            internal static readonly ReflectedType Type;
        }
    }
}
