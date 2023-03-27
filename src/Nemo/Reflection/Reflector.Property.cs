using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Collections.Concurrent;
using Nemo.Configuration;

namespace Nemo.Reflection
{
    public static partial class Reflector
    {
        public static class Property
        {
            private static readonly ConcurrentDictionary<Tuple<Type, string>, GenericGetter> Getters = new ConcurrentDictionary<Tuple<Type, string>, GenericGetter>();
            private static readonly ConcurrentDictionary<Tuple<Type, string>, Tuple<GenericSetter, Type>> Setters = new ConcurrentDictionary<Tuple<Type, string>, Tuple<GenericSetter, Type>>();
            
            #region Property Getters

            public static object Get<T>(T target, string propertyName)
            {
                return Get(typeof(T), target, propertyName);
            }

            public static TResult Get<T, TResult>(T target, string propertyName)
            {
                return (TResult)Get(typeof(T), target, propertyName);
            }

            public static object Get(Type targetType, object target, string propertyName)
            {
                if (target != null)
                {
                    var propertyKey = Tuple.Create(targetType, propertyName);
                    var getMethod = Getters.GetOrAdd(propertyKey, GenerateGetter);
                    return getMethod(target);
                }
                return null;
            }

            private static GenericGetter GenerateGetter(Tuple<Type, string> key)
            {
                return CreateGetMethod(GetProperty(key.Item1, key.Item2), key.Item1);
            }

            #endregion

            #region Property Setters

            public static void Set<T>(T target, string propertyName, object value)
            {
                Set(typeof(T), target, propertyName, value);
            }

            public static void Set<T, TValue>(T target, string propertyName, TValue value)
            {
                Set(typeof(T), target, propertyName, value);
            }

            public static void Set(Type targetType, object target, string propertyName, object value)
            {
                if (target != null)
                {
                    var propertyKey = Tuple.Create(targetType, propertyName);
                    var setter = Setters.GetOrAdd(propertyKey, GenerateSetter);

                    if (value != null && value is IConvertible && value.GetType() != setter.Item2)
                    {
                        setter.Item1(target, ChangeType(value, setter.Item2));
                    }
                    else
                    {
                        setter.Item1(target, value);
                    }                    
                }
            }

            private static Tuple<GenericSetter, Type> GenerateSetter(Tuple<Type, string> key)
            {
                var property = GetProperty(key.Item1, key.Item2);
                return Tuple.Create(CreateSetMethod(property, key.Item1), property.PropertyType);
            }

            #endregion

            #region Getter/Setter Methods

            public delegate void GenericSetter(object target, object value);
            public delegate object GenericGetter(object target);

            /// <summary>
            /// Creates a dynamic setter for the property
            /// </summary>
            /// <param name="propertyInfo"></param>
            /// <param name="targetType"></param>
            private static GenericSetter CreateSetMethod(PropertyInfo propertyInfo, Type targetType)
            {
                var setMethod = propertyInfo.GetSetMethod();
                if (setMethod == null)
                {
                    return null;
                }

                var setter = new DynamicMethod("Set_" + propertyInfo.Name, typeof(void), new[] { typeof(object), typeof(object) }, typeof(ObjectFactory), true);
                var generator = setter.GetILGenerator();
                if (!setMethod.IsStatic)
                {
                    generator.PushInstance(propertyInfo.DeclaringType);
                }

                generator.Emit(OpCodes.Ldarg_1);
                generator.UnboxIfNeeded(propertyInfo.PropertyType);

                if (setMethod.IsFinal || !setMethod.IsVirtual)
                {
                    generator.Emit(OpCodes.Call, setMethod);
                }
                else
                {
                    generator.Emit(OpCodes.Callvirt, setMethod);
                }
                generator.Emit(OpCodes.Ret);

                return (GenericSetter)setter.CreateDelegate(typeof(GenericSetter));
            }

            /// <summary>
            /// Creates a dynamic getter for the property
            /// </summary>
            /// <param name="propertyInfo"></param>
            /// <returns></returns>
            private static GenericGetter CreateGetMethod(PropertyInfo propertyInfo, Type targetType)
            {
                var getMethod = propertyInfo.GetGetMethod();
                if (getMethod == null)
                {
                    return null;
                }

                var getter = new DynamicMethod("Get_" + propertyInfo.Name, typeof(object), new[] { typeof(object) }, typeof(ObjectFactory), true);
                var generator = getter.GetILGenerator();
                if (!getMethod.IsStatic)
                {
                    generator.PushInstance(propertyInfo.DeclaringType);
                }

                if (getMethod.IsFinal || !getMethod.IsVirtual)
                {
                    generator.Emit(OpCodes.Call, getMethod);
                }
                else
                {
                    generator.Emit(OpCodes.Callvirt, getMethod);
                }

                generator.BoxIfNeeded(propertyInfo.PropertyType);
                generator.Emit(OpCodes.Ret);

                return (GenericGetter)getter.CreateDelegate(typeof(GenericGetter));
            }

            #endregion
        }
    }
}
