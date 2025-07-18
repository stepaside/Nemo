﻿using Nemo.Attributes.Converters;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Nemo.Reflection
{
    public static class Mapper
    {
        public delegate void PropertyMapper(object source, object target);
        public delegate void DictionaryMapper(object source, IDictionary<string, object> target);

        private static readonly ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper> Mappers = new ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper>();
        private static readonly ConcurrentDictionary<Type, DictionaryMapper> DictionaryMappers = new ConcurrentDictionary<Type, DictionaryMapper>();

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
