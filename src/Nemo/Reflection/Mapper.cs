using Nemo.Attributes.Converters;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace Nemo.Reflection
{
    public static class Mapper
    {
        public delegate void PropertyMapper(object source, object target);
        public delegate void DictionaryMapper(object source, IDictionary<string, object> target);

        private static readonly ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper> Mappers = new ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper>();
        private static readonly ConcurrentDictionary<Type, DictionaryMapper> DictionaryMappers = new ConcurrentDictionary<Type, DictionaryMapper>();
        private static readonly ConcurrentDictionary<Tuple<Type, bool, string>, PropertyMapper> ReaderMappers = new ConcurrentDictionary<Tuple<Type, bool, string>, PropertyMapper>();

        private static readonly MethodInfo IsDBNullMethod = typeof(IDataRecord).GetMethod("IsDBNull", new[] { typeof(int) });

        private static readonly Dictionary<Type, MethodInfo> TypedGetters = new Dictionary<Type, MethodInfo>
        {
            { typeof(bool), typeof(IDataRecord).GetMethod("GetBoolean", new[] { typeof(int) }) },
            { typeof(byte), typeof(IDataRecord).GetMethod("GetByte", new[] { typeof(int) }) },
            { typeof(char), typeof(IDataRecord).GetMethod("GetChar", new[] { typeof(int) }) },
            { typeof(DateTime), typeof(IDataRecord).GetMethod("GetDateTime", new[] { typeof(int) }) },
            { typeof(decimal), typeof(IDataRecord).GetMethod("GetDecimal", new[] { typeof(int) }) },
            { typeof(double), typeof(IDataRecord).GetMethod("GetDouble", new[] { typeof(int) }) },
            { typeof(float), typeof(IDataRecord).GetMethod("GetFloat", new[] { typeof(int) }) },
            { typeof(Guid), typeof(IDataRecord).GetMethod("GetGuid", new[] { typeof(int) }) },
            { typeof(short), typeof(IDataRecord).GetMethod("GetInt16", new[] { typeof(int) }) },
            { typeof(int), typeof(IDataRecord).GetMethod("GetInt32", new[] { typeof(int) }) },
            { typeof(long), typeof(IDataRecord).GetMethod("GetInt64", new[] { typeof(int) }) },
            { typeof(string), typeof(IDataRecord).GetMethod("GetString", new[] { typeof(int) }) }
        };

        private static readonly Dictionary<Type, MethodInfo> GetItemMethods = typeof(MappingFactory).GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public).Where(m => m.Name == "GetItem").ToDictionary(m => m.GetParameters()[0].ParameterType, m => m);

        internal static PropertyMapper CreateDelegate(Type sourceType, Type targetType, bool indexer, bool autoTypeCoercion)
        {
            var key = Tuple.Create(sourceType, targetType, indexer, autoTypeCoercion);
            var mapper = Mappers.GetOrAdd(key, t => t.Item3 ? GenerateIndexerDelegate(t.Item1, t.Item2, t.Item4) : GenerateDelegate(t.Item1, t.Item2));
            return mapper;
        }

        internal static DictionaryMapper CreateDelegate(Type sourceType)
        {
            var mapper = DictionaryMappers.GetOrAdd(sourceType, t => GenerateDictionaryMapperDelegate(t));
            return mapper;
        }

        /// <summary>
        /// Creates a mapper which reads values from a data record by ordinal. The mapper is cached per result set shape
        /// (column names and field types), target type and type coercion mode.
        /// </summary>
        internal static PropertyMapper CreateReaderDelegate(IDataRecord record, Type targetType, bool autoTypeCoercion)
        {
            var count = record.FieldCount;
            var columnNames = new string[count];
            var fieldTypes = new Type[count];
            var shape = new StringBuilder();
            for (var i = 0; i < count; i++)
            {
                columnNames[i] = record.GetName(i);
                fieldTypes[i] = GetFieldType(record, i);
                shape.Append(columnNames[i]).Append('\u0002').Append(fieldTypes[i]?.FullName).Append('\u0001');
            }
            var key = Tuple.Create(targetType, autoTypeCoercion, shape.ToString());
            return ReaderMappers.GetOrAdd(key, t => GenerateReaderDelegate(t.Item1, columnNames, fieldTypes, t.Item2));
        }

        private static Type GetFieldType(IDataRecord record, int ordinal)
        {
            try
            {
                return record.GetFieldType(ordinal);
            }
            catch (Exception)
            {
                return null;
            }
        }

        private static PropertyMapper GenerateReaderDelegate(Type targetType, string[] columnNames, Type[] fieldTypes, bool autoTypeCoercion)
        {
            var method = new DynamicMethod("Map_Reader_" + targetType.FullName, null, new[] { typeof(object), typeof(object) }, typeof(Mapper).Module);
            var il = method.GetILGenerator();

            var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < columnNames.Length; i++)
            {
                if (!ordinals.ContainsKey(columnNames[i]))
                {
                    ordinals.Add(columnNames[i], i);
                }
            }

            var targetProperties = Reflector.GetPropertyMap(targetType);
            var entityMap = MappingFactory.GetEntityMap(targetType);
            var getValue = typeof(Mapper).GetMethod(nameof(GetValue), BindingFlags.Static | BindingFlags.NonPublic);

            var matches = targetProperties.Where(t => t.Value.IsSelectable && t.Key.PropertyType.IsPublic && t.Key.CanWrite && t.Key.CanRead && (t.Value.IsSimpleList || t.Value.IsSimpleType || t.Value.IsBinary));
            foreach (var match in matches)
            {
                var declaredConverter = MappingFactory.GetTypeConverter(typeof(object), match.Key, entityMap);

                if (match.Value.IsSimpleList && declaredConverter == null) continue;
                var typeConverter = MatchTypeConverter(targetType, match.Value, typeof(object), declaredConverter, autoTypeCoercion);

                var propertyName = MappingFactory.GetPropertyOrColumnName(match.Key, false, entityMap, true);
                if (propertyName == null || !ordinals.TryGetValue(propertyName, out var ordinal)) continue;

                //  When no converter is declared for the property and the field type already matches the property type
                //  the value can be read with a typed accessor, skipping boxing and conversion altogether
                if (declaredConverter?.Item1 == null && EmitTypedRead(il, match.Key, fieldTypes[ordinal], ordinal))
                {
                    continue;
                }

                il.Emit(OpCodes.Ldarg_1);
                if (typeConverter.Item1 != null)
                {
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Castclass, typeof(IDataRecord));
                il.EmitFastInt(ordinal);

                //  Default value is the current property value, matching the indexer-based mapper
                var propertyType = match.Value.PropertyType;
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Callvirt, match.Key.GetGetMethod());
                if (propertyType.IsValueType)
                {
                    il.BoxIfNeeded(propertyType);
                }
                else
                {
                    il.Emit(OpCodes.Castclass, typeof(object));
                }

                il.Emit(OpCodes.Call, getValue);

                if (typeConverter.Item1 == null)
                {
                    il.EmitCastToReference(match.Key.PropertyType);
                }
                else
                {
                    il.Emit(OpCodes.Callvirt, typeConverter.Item2.GetMethod("ConvertForward"));
                }
                il.EmitCall(OpCodes.Callvirt, match.Key.GetSetMethod(), null);
            }
            il.Emit(OpCodes.Ret);

            var mapper = (PropertyMapper)method.CreateDelegate(typeof(PropertyMapper));
            return mapper;
        }

        private static bool EmitTypedRead(ILGenerator il, PropertyInfo property, Type fieldType, int ordinal)
        {
            if (fieldType == null) return false;

            var propertyType = property.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propertyType);
            if (fieldType != (underlyingType ?? propertyType)) return false;
            if (!TypedGetters.TryGetValue(fieldType, out var getter)) return false;

            var isNull = il.DefineLabel();
            var done = il.DefineLabel();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, typeof(IDataRecord));
            il.EmitFastInt(ordinal);
            il.Emit(OpCodes.Callvirt, IsDBNullMethod);
            il.Emit(OpCodes.Brtrue, isNull);

            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, typeof(IDataRecord));
            il.EmitFastInt(ordinal);
            il.Emit(OpCodes.Callvirt, getter);
            if (underlyingType != null)
            {
                il.Emit(OpCodes.Newobj, propertyType.GetConstructor(new[] { underlyingType }));
            }
            il.Emit(OpCodes.Callvirt, property.GetSetMethod());
            il.Emit(OpCodes.Br, done);

            //  A null value assigns the default value of the property type
            il.MarkLabel(isNull);
            il.Emit(OpCodes.Ldarg_1);
            if (propertyType.IsValueType)
            {
                var local = il.DeclareLocal(propertyType);
                il.Emit(OpCodes.Ldloca_S, local);
                il.Emit(OpCodes.Initobj, propertyType);
                il.Emit(OpCodes.Ldloc, local);
            }
            else
            {
                il.Emit(OpCodes.Ldnull);
            }
            il.Emit(OpCodes.Callvirt, property.GetSetMethod());

            il.MarkLabel(done);
            return true;
        }

        internal static object GetValue(IDataRecord record, int ordinal, object defaultValue)
        {
            var value = record.GetValue(ordinal);
            return value == null || value is DBNull ? defaultValue : value;
        }

        private static PropertyMapper GenerateDelegate(Type sourceType, Type targetType)
        {
            var method = new DynamicMethod("Map_" + sourceType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) }, true);
            var il = method.GetILGenerator();

            var sourceProperties = Reflector.GetAllProperties(sourceType);
            var targetProperties = Reflector.GetAllProperties(targetType);

            var entityMap = MappingFactory.GetEntityMap(targetType);

            var matches = sourceProperties.CrossJoin(targetProperties).Where(t => (t.Item2.Name == t.Item3.Name || t.Item2.Name == MappingFactory.GetPropertyOrColumnName(t.Item3, false, entityMap, false))
                                                                                    && t.Item2.PropertyType == t.Item3.PropertyType
                                                                                    && t.Item2.PropertyType.IsPublic
                                                                                    && t.Item3.PropertyType.IsPublic
                                                                                    //&& (t.Item3.PropertyType.IsValueType || t.Item3.PropertyType == typeof(string))
                                                                                    && t.Item2.CanRead && t.Item3.CanWrite);
                
            foreach (var match in matches)
            {
                il.Emit(OpCodes.Ldarg_1);
                il.EmitCastToReference(targetType);
                il.Emit(OpCodes.Ldarg_0);
                il.EmitCastToReference(sourceType);
                il.Emit(OpCodes.Callvirt, match.Item2.GetGetMethod());
                il.Emit(OpCodes.Callvirt, match.Item3.GetSetMethod());
            }
            il.Emit(OpCodes.Ret);

            var mapper = (PropertyMapper)method.CreateDelegate(typeof(PropertyMapper));
            return mapper;
        }

        private static PropertyMapper GenerateIndexerDelegate(Type indexerType, Type targetType, bool autoTypeCoercion)
        {
            var method = new DynamicMethod("Map_" + indexerType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) }, typeof(Mapper).Module);
            var il = method.GetILGenerator();

            var targetProperties = Reflector.GetPropertyMap(targetType);
            var entityMap = MappingFactory.GetEntityMap(targetType);
            var getTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");
            var getDefaultValue = typeof(System.Activator).GetMethods().FirstOrDefault(m => m.Name == "CreateInstance" && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(Type));

            var useIndexerMethod = true;
            if (!GetItemMethods.TryGetValue(indexerType, out var getItem) || getItem == null)
            {
                getItem = indexerType.GetMethod("get_Item", new[] { typeof(string) });
                useIndexerMethod = false;
            }

            var matches = targetProperties.Where(t => t.Value.IsSelectable && t.Key.PropertyType.IsPublic && t.Key.CanWrite && (t.Value.IsSimpleList || t.Value.IsSimpleType || t.Value.IsBinary));
            foreach (var match in matches)
            {
                var typeConverter = MappingFactory.GetTypeConverter(getItem.ReturnType, match.Key, entityMap);

                if (match.Value.IsSimpleList && typeConverter == null) continue;
                typeConverter = MatchTypeConverter(targetType, match.Value, getItem.ReturnType, typeConverter, autoTypeCoercion);

                var propertyName = MappingFactory.GetPropertyOrColumnName(match.Key, false, entityMap, true);
                if (propertyName == null) continue;

                il.Emit(OpCodes.Ldarg_1);
                if (typeConverter.Item1 != null)
                {
                    //	New the converter
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, propertyName);
                if (!useIndexerMethod)
                {
                    il.Emit(OpCodes.Callvirt, getItem);
                }
                else
                {
                    var propertyType = match.Value.PropertyType;
                    if (propertyType.IsValueType)
                    {
                        //var ctor = propertyType.GetConstructor(Type.EmptyTypes);
                        //if (ctor != null)
                        //{
                        //    il.Emit(OpCodes.Newobj, ctor);
                        //    il.BoxIfNeeded(propertyType);
                        //}
                        //else if (propertyType.IsPrimitive)
                        //{
                        //    if (propertyType == typeof(double))
                        //    {
                        //        il.Emit(OpCodes.Ldc_R8, 0.0);
                        //    }
                        //    else if (propertyType == typeof(float))
                        //    {
                        //        il.Emit(OpCodes.Ldc_R4, 0.0f);
                        //    }
                        //    else
                        //    {
                        //        il.EmitFastInt(0);
                        //        if (propertyType == typeof(long) || propertyType == typeof(ulong))
                        //        {
                        //            il.Emit(OpCodes.Conv_I8);
                        //        }
                        //    }
                        //    il.BoxIfNeeded(propertyType);
                        //}
                        //else
                        //{
                        //    il.Emit(OpCodes.Ldtoken, propertyType);
                        //    il.Emit(OpCodes.Call, getTypeFromHandle);
                        //    il.Emit(OpCodes.Call, getDefaultValue);
                        //}
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Call, match.Key.GetGetMethod());
                        il.BoxIfNeeded(propertyType);
                    }
                    else
                    {
                        //il.Emit(OpCodes.Ldnull);
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Call, match.Key.GetGetMethod());
                        il.Emit(OpCodes.Castclass, typeof(object));
                    }
                    il.Emit(OpCodes.Call, getItem);
                }
                if (typeConverter.Item1 == null)
                {
                    il.EmitCastToReference(match.Key.PropertyType);
                }
                else
                {
                    //	Call the convert method
                    il.Emit(OpCodes.Callvirt, typeConverter.Item2.GetMethod("ConvertForward"));
                }
                il.EmitCall(OpCodes.Callvirt, match.Key.GetSetMethod(), null);
            }
            il.Emit(OpCodes.Ret);

            var mapper = (PropertyMapper)method.CreateDelegate(typeof(PropertyMapper));
            return mapper;
        }

        private static DictionaryMapper GenerateDictionaryMapperDelegate(Type sourceType)
        {
            var method = new DynamicMethod("Map_ToDictionary" + sourceType.FullName, null, new[] { typeof(object), typeof(IDictionary<string, object>) }, typeof(Mapper).Module, true);
            var il = method.GetILGenerator();

            var setItem = typeof(IDictionary<string, object>).GetMethod("set_Item", new[] { typeof(string), typeof(object) });
            var sourceProperties = sourceType.GetProperties();
            foreach (var property in sourceProperties)
            {
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldstr, property.Name);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Callvirt, property.GetGetMethod());
                il.BoxIfNeeded(property.PropertyType);
                il.EmitCall(OpCodes.Callvirt, setItem, null);
            }
            il.Emit(OpCodes.Ret);

            var mapper = (DictionaryMapper)method.CreateDelegate(typeof(DictionaryMapper));
            return mapper;
        }

        private static Tuple<Type, Type> MatchTypeConverter(Type targetType, ReflectedProperty property, Type fromType, Tuple<Type, Type> typeConverter, bool autoTypeCoercion)
        {
            if (typeConverter.Item1 == null && autoTypeCoercion)
            {
                var interfaceType = typeConverter.Item2;
                if (interfaceType == null)
                {
                    interfaceType = TypeConverterAttribute.GetExpectedConverterInterfaceType(fromType, property.PropertyType);
                }

                if (property.PropertyType == typeof(string))
                {
                    return Tuple.Create(typeof(DBNullableStringConverter), interfaceType);
                }
                else if (property.IsNullableType)
                {
                    if (property.PropertyType.IsEnum)
                    {
                        var propertyType = property.PropertyType.GetEnumUnderlyingType();
                        return Tuple.Create(typeof(NullableEnumConverter<>).MakeGenericType(propertyType), interfaceType);
                    }
                    else
                    {
                        var propertyType = Nullable.GetUnderlyingType(property.PropertyType);
                        return Tuple.Create(typeof(DBNullableTypeConverter<>).MakeGenericType(propertyType), interfaceType);
                    }
                }
                else if (property.IsSimpleType && property.PropertyType.IsEnum)
                {
                    return Tuple.Create(typeof(EnumConverter<>).MakeGenericType(property.PropertyType), interfaceType);
                }
                else if (property.PropertyType == typeof(byte[]))
                {
                    return Tuple.Create(typeof(DBNullableByteArrayConverter).MakeGenericType(property.PropertyType), interfaceType);
                }
                else if (property.IsSimpleType)
                {
                    return Tuple.Create(typeof(ThrowingSimpleTypeConverter<>).MakeGenericType(property.PropertyType), interfaceType);
                }
            }
            return typeConverter;
        }
    }
}
