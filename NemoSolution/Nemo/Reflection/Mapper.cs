using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.Emit;
using Nemo.Attributes;
using Nemo.Collections.Extensions;
using Nemo.Attributes.Converters;
using System.Reflection;
using Nemo.Fn;
using Nemo.Configuration.Mapping;

namespace Nemo.Reflection
{
    public static class Mapper
    {
        public delegate void PropertyMapper(object source, object target);
        private static ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper> _mappers = new ConcurrentDictionary<Tuple<Type, Type, bool, bool>, PropertyMapper>();

        internal static PropertyMapper CreateDelegate(Type sourceType, Type targetType, bool indexer, bool ignoreMappings)
        {
            var key = Tuple.Create(sourceType, targetType, indexer, ignoreMappings);
            var mapper = _mappers.GetOrAdd(key, t => t.Item3 ? GenerateIndexerDelegate(t.Item1, t.Item2, t.Item4) : GenerateDelegate(t.Item1, t.Item2, t.Item4));
            return mapper;
        }

        private static PropertyMapper GenerateDelegate(Type sourceType, Type targetType, bool ignoreMappings)
        {
            var method = new DynamicMethod("Map_" + sourceType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) });
            var il = method.GetILGenerator();

            var sourceProperties = Reflector.GetAllProperties(sourceType);
            var targetProperties = Reflector.GetAllProperties(targetType);

            var entityMap = MappingFactory.GetEntityMap(targetType);

            var matches = sourceProperties.CrossJoin(targetProperties).Where(t => t.Item2.Name == MappingFactory.GetPropertyOrColumnName(t.Item3, ignoreMappings, entityMap, false)
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

        private static PropertyMapper GenerateIndexerDelegate(Type indexerType, Type targetType, bool ignoreMappings)
        {
            var method = new DynamicMethod("Map_" + indexerType.FullName + "_" + targetType.FullName, null, new[] { typeof(object), typeof(object) }, typeof(Mapper).Module);
            var il = method.GetILGenerator();

            var targetProperties = Reflector.GetPropertyMap(targetType);
            var entityMap = MappingFactory.GetEntityMap(targetType);

            var getItem = indexerType.GetMethod("get_Item", new Type[] { typeof(string) });

            var matches = targetProperties.Where(t => t.Value.IsSelectable && t.Key.PropertyType.IsPublic && t.Key.CanWrite && (t.Value.IsSimpleType || t.Value.IsBinary));
            foreach (var match in matches)
            {
                var typeConverter = TypeConverterAttribute.GetTypeConverter(getItem.ReturnType, match.Key);

                il.Emit(OpCodes.Ldarg_1);
                if (typeConverter.Item1 != null)
                {
                    //	New the converter
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, MappingFactory.GetPropertyOrColumnName(match.Key, ignoreMappings, entityMap, true));
                il.Emit(OpCodes.Callvirt, getItem);
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
    }
}
