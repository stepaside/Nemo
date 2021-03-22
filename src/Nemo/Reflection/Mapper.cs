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
        private static readonly ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper> Mappers = new ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper>();
        private static readonly Dictionary<Type, MethodInfo> GetItemMethods = typeof(MappingFactory).GetMethods(BindingFlags.Static | BindingFlags.NonPublic).Where(m => m.Name == "GetItem").ToDictionary(m => m.GetParameters()[0].ParameterType, m => m);

        internal static PropertyMapper CreateDelegate(Type sourceType, Type targetType, bool indexer, bool strict)
        {
            var key = Tuple.Create(sourceType, targetType, indexer, strict);
            var mapper = Mappers.GetOrAdd(key, t => t.Item3 ? GenerateIndexerDelegate(t.Item1, t.Item2, t.Item4) : GenerateDelegate(t.Item1, t.Item2, t.Item4));
            return mapper;
        }

        private static PropertyMapper GenerateDelegate(Type sourceType, Type targetType, bool strict)
        {
            var method = new DynamicMethod("Map_" + sourceType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) });
            var il = method.GetILGenerator();

            var sourceProperties = Reflector.GetAllProperties(sourceType);
            var targetProperties = Reflector.GetAllProperties(targetType);

            var entityMap = MappingFactory.GetEntityMap(targetType);

            var matches = sourceProperties.CrossJoin(targetProperties).Where(t => (t.Item2.Name == t.Item3.Name || t.Item2.Name == MappingFactory.GetPropertyOrColumnName(t.Item3, !strict, entityMap, false))
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

        private static PropertyMapper GenerateIndexerDelegate(Type indexerType, Type targetType, bool strict)
        {
            var method = new DynamicMethod("Map_" + indexerType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) }, typeof(Mapper).Module);
            var il = method.GetILGenerator();

            var targetProperties = Reflector.GetPropertyMap(targetType);
            var entityMap = MappingFactory.GetEntityMap(targetType);

            if (strict || !GetItemMethods.TryGetValue(indexerType, out var getItem) || getItem == null)
            {
                getItem = indexerType.GetMethod("get_Item", new[] { typeof(string) });
                strict = true;
            }

            var matches = targetProperties.Where(t => t.Value.IsSelectable && t.Key.PropertyType.IsPublic && t.Key.CanWrite && (t.Value.IsSimpleList || t.Value.IsSimpleType || t.Value.IsBinary));
            foreach (var match in matches)
            {
                var typeConverter = MappingFactory.GetTypeConverter(getItem.ReturnType, match.Key, entityMap);

                if (match.Value.IsSimpleList && typeConverter == null) continue;
                typeConverter = MatchTypeConverter(targetType, match.Value, getItem.ReturnType, typeConverter);

                il.Emit(OpCodes.Ldarg_1);
                if (typeConverter.Item1 != null)
                {
                    //	New the converter
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, MappingFactory.GetPropertyOrColumnName(match.Key, false, entityMap, true));
                if (strict)
                {
                    il.Emit(OpCodes.Callvirt, getItem);
                }
                else
                {
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

        private static Tuple<Type, Type> MatchTypeConverter(Type targetType, ReflectedProperty property, Type fromType, Tuple<Type, Type> typeConverter)
        {
            if (typeConverter.Item1 == null && (ConfigurationFactory.Get(targetType)?.AutoTypeCoercion).GetValueOrDefault())
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
            }
            return typeConverter;
        }
    }
}
