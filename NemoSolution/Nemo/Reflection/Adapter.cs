using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Nemo.Extensions;
using System.Collections.Concurrent;
using Nemo.Attributes;
using Nemo.Attributes.Converters;
using ObjectActivator = Nemo.Reflection.Activator.ObjectActivator;
using Nemo.Configuration.Mapping;

namespace Nemo.Reflection
{
    public sealed class Adapter
    {
        private static readonly ConcurrentDictionary<Tuple<Type, Type, string>, Type> Types = new ConcurrentDictionary<Tuple<Type, Type, string>, Type>();

        public static T Bind<T>(object value)
            where T : class
        {
            if (!typeof(T).IsInterface)
            {
                throw new ArgumentException("The given type is not an interface.");
            }
            var activator = InternalBind<T>(value.GetType());
            return (T)activator(value);
        }

        public static T Guard<T>(T value)
        {
            if (!typeof(T).IsInterface)
            {
                throw new ArgumentException("The given type is not an interface.");
            }
            var activator = InternalGuard<T>();
            return (T)activator(value);
        }

        public static T Implement<T>()
            where T : class
        {
            if (!typeof(T).IsInterface)
            {
                throw new ArgumentException("The given type is not an interface.");
            }
            var activator = FastImplementor<T>.Instance;
            return (T)activator();
        }

        public static object Wrap(object value, Type interfaceType, bool ignoreMappings, bool includeAllProperties)
        {
            if (!interfaceType.IsInterface)
            {
                throw new ArgumentException("The given type is not an interface.");
            }
            var activator = InternalWrap(value.GetType(), interfaceType, ignoreMappings, includeAllProperties);
            return activator(value);
        }

        internal static ObjectActivator InternalBind<T>(Type objectType)
        {
            var interfaceType = typeof(T);
            var key = Tuple.Create(objectType, interfaceType, "Adapter");
            var type = Types.GetOrAdd(key, n =>
            {
                var isAnonymous = Reflector.IsAnonymousType(n.Item1);
                var name = string.Format("{0}_{1}_{2}", isAnonymous ? n.Item1.Name : n.Item1.FullName, n.Item2.FullName, n.Item3);
                // creates the assembly and module.
                var builder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.Run);
                var module = builder.DefineDynamicModule(name);
                // create the type that is used to wrap the object given. This
                // type will also implement the interface.
                return CreateType(objectType, name, interfaceType, module, DynamicProxyType.Adapter, false);
            });

            var activator = Activator.CreateDelegate(type, objectType);
            return activator;
        }

        internal static ObjectActivator InternalGuard<T>()
        {
            var interfaceType = typeof(T);
            var key = Tuple.Create(interfaceType, interfaceType, "Guard");
            var type = Types.GetOrAdd(key, n =>
            {
                var name = string.Format("{0}_{1}", n.Item1.FullName, n.Item3);
                // creates the assembly and module.
                var builder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.Run);
                var module = builder.DefineDynamicModule(name);
                // create the type that is used to wrap the object given. This
                // type will also implement the interface.
                return CreateType(interfaceType, name, interfaceType, module, DynamicProxyType.Guard, false);
            });

            var activator = Activator.CreateDelegate(type, interfaceType);
            return activator;
        }

        internal static ObjectActivator InternalImplement<T>()
        {
            var interfaceType = typeof(T);
            var key = Tuple.Create(interfaceType, interfaceType, "Implementation");
            var type = Types.GetOrAdd(key, n =>
            {
                var name = string.Format("{0}_{1}", n.Item1.FullName, n.Item3);
                // creates the assembly and module.
                var builder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.Run);
                var module = builder.DefineDynamicModule(name);
                // create the type that is used to wrap the object given. This
                // type will also implement the interface.
                return CreateType(name, interfaceType, module);
            });

            var activator = Activator.CreateDelegate(type);
            return activator;
        }

        internal static ObjectActivator InternalWrap(Type objectType, Type interfaceType, bool ignoreMappings, bool fullIndexer)
        {
            var suffix = "Wrap";
            if (ignoreMappings)
            {
                suffix = String.Concat(suffix, "_Exact");
            }
            if (fullIndexer)
            {
                suffix = String.Concat(suffix, "_All");
            }
            var key = Tuple.Create(interfaceType, interfaceType, suffix);
            var type = Types.GetOrAdd(key, n =>
            {
                var name = string.Format("{0}_{1}", n.Item1.FullName, n.Item3);
                // creates the assembly and module.
                var builder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.Run);
                var module = builder.DefineDynamicModule(name);
                // create the type that is used to wrap the object given. This
                // type will also implement the interface.
                return CreateType(objectType, name, interfaceType, module, (fullIndexer ? DynamicProxyType.FullIndexer : DynamicProxyType.SimpleIndexer), ignoreMappings);
            });

            var activator = Activator.CreateDelegate(type, objectType);
            return activator;
        }

        internal enum DynamicProxyType { Adapter, SimpleIndexer, FullIndexer, Guard, Implementation }

        internal static Type CreateType(Type objectType, string typeName, Type interfaceType, ModuleBuilder module, DynamicProxyType proxyType, bool ignoreMappings)
        {
            // create the type that is used to wrap the object into the interface.
            var typeBuilder = module.DefineType(typeName, TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.AutoClass | TypeAttributes.AnsiClass | TypeAttributes.BeforeFieldInit | TypeAttributes.AutoLayout | TypeAttributes.Sealed);

            // add the interface implementation to the type.
            typeBuilder.AddInterfaceImplementation(interfaceType);

            if (proxyType == DynamicProxyType.Guard)
            {
                // ReSharper disable once AssignNullToNotNullAttribute
                typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(typeof(ReadOnlyAttribute).GetConstructor(Type.EmptyTypes), new object[] { }));
            }

            //typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(typeof(SerializableAttribute).GetConstructor(Type.EmptyTypes), new object[] { })); 

            if (proxyType == DynamicProxyType.SimpleIndexer || proxyType == DynamicProxyType.FullIndexer)
            {
                // holds the object that is wrapped.
                var field = typeBuilder.DefineField("_original", objectType, FieldAttributes.Private);
                DefineReadOnlyProperty("Indexer", typeBuilder, field);

                // create the constructor for the type.
                DefineConstructor(objectType, typeBuilder, field);

                // define properties for the type
                DefineProperties(objectType, typeBuilder, field, interfaceType, proxyType, ignoreMappings);
            }
            else
            {
                // holds the object that is wrapped.
                var field = typeBuilder.DefineField("_original", objectType, FieldAttributes.Private);

                // create the constructor for the type.
                DefineConstructor(objectType, typeBuilder, field);

                // define properties for the type
                DefineProperties(objectType, typeBuilder, field, interfaceType, proxyType, false);

                // define some methods that are found on system.object as long as we do not create a guarded object
                if (proxyType != DynamicProxyType.Guard)
                {
                    DefineMethod(objectType.GetMethod("ToString"), typeBuilder, field, objectType);
                    DefineMethod(objectType.GetMethod("Equals"), typeBuilder, field, objectType);
                    DefineMethod(objectType.GetMethod("GetType"), typeBuilder, field, objectType);
                    DefineMethod(objectType.GetMethod("GetHashCode"), typeBuilder, field, objectType);
                }
            }

            // create the final type. 
            var type = typeBuilder.CreateType();
            return type;
        }

        internal static Type CreateType(string typeName, Type interfaceType, ModuleBuilder module)
        {
            // create the type that is used to wrap the object into the interface.
            var typeBuilder = module.DefineType(typeName, TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.AutoClass | TypeAttributes.AnsiClass | TypeAttributes.BeforeFieldInit | TypeAttributes.AutoLayout | TypeAttributes.Sealed);

            // add the interface implementation to the type.
            typeBuilder.AddInterfaceImplementation(interfaceType);

            //typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(typeof(SerializableAttribute).GetConstructor(Type.EmptyTypes), new object[] { })); 

            // create the constructor for the type.
            typeBuilder.DefineDefaultConstructor(MethodAttributes.Public);

            // define properties for the type
            DefineProperties(null, typeBuilder, null, interfaceType, DynamicProxyType.Implementation, false);

            // create the final type. 
            var type = typeBuilder.CreateType();

            return type;
        }

        private static void DefineMethod(MethodInfo method, TypeBuilder typeBuilder, FieldInfo field, Type objectType)
        {
            const MethodAttributes attributes = MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.Virtual;
            var parameterTypes = method.GetParameters().Select(x => x.ParameterType).ToArray();

            // create the new method.
            var methodBuilder = typeBuilder.DefineMethod(method.Name, attributes, method.ReturnType, parameterTypes);

            // get the IL generator to generate the required IL.
            ILGenerator il = methodBuilder.GetILGenerator();

            // load the first argument (the instance itself) and the field.
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, field);

            // check if the inner type is public and in the same assembly.
            if (!objectType.IsPublic)
            {
                // required to create some reflection code to access internal and outside of the
                // assembly properties.
                il.EmitCall(OpCodes.Callvirt, objectType.GetMethod("GetType"), null);

                // get the property.
                il.Emit(OpCodes.Ldstr, method.Name);
                il.EmitCall(OpCodes.Callvirt, typeof(Type).GetMethod("GetMethod", new[] { typeof(string) }), null);

                // load the arguments for the next method call.
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, field);

                // push the parameters for the method call onto the stack.
                if (parameterTypes.Length == 0)
                    il.Emit(OpCodes.Ldnull);
                else
                {
                    for (int i = 0; i < parameterTypes.Length; i++)
                    {
                        il.Emit(OpCodes.Ldarg, i + 1);
                    }
                }

                il.EmitCall(OpCodes.Callvirt, typeof(MethodInfo).GetMethod("Invoke", new[] { typeof(object), typeof(object[]) }), null);

                // cast or unbox if required.
                il.EmitCastToReference(method.ReturnType);
            }
            else
            {
                // push the parameters for the method call onto the stack.
                if (parameterTypes.Length > 0)
                {
                    for (int i = 0; i < parameterTypes.Length; i++)
                    {
                        il.Emit(OpCodes.Ldarg, i + 1);
                    }
                }

                // directly call the inner object's get method of the property.
                il.EmitCall(OpCodes.Callvirt, method, null);
            }

            il.Emit(OpCodes.Ret);
        }

        private static void DefineProperties(Type objectType, TypeBuilder typeBuilder, FieldInfo field, Type interfaceType, DynamicProxyType proxyType, bool ignoreMappings)
        {
            var entityMap = MappingFactory.GetEntityMap(interfaceType);

            foreach (var property in Reflector.GetAllProperties(interfaceType))
            {
                // check if we can support the wrapping.
                var propertyName = MappingFactory.GetPropertyOrColumnName(property, ignoreMappings, entityMap, false);
                var objectProperty = objectType != null ? objectType.GetProperty(propertyName) : null;

                if (objectProperty != null && ((property.CanRead && !objectProperty.CanRead) || (property.CanWrite && !objectProperty.CanWrite)))
                {
                    throw new InvalidCastException("Can't cast because the property is missing or does not have the required implementation.");
                }

                // check the property types.
                if (objectProperty != null && objectProperty.PropertyType != property.PropertyType)
                    throw new InvalidCastException("Can't cast because property types do not match.");

                // define the property.
                if (proxyType == DynamicProxyType.FullIndexer)
                {
                    DefineIndexerProperty(property, typeBuilder, field, objectType, ignoreMappings, entityMap);
                }
                else if (proxyType == DynamicProxyType.SimpleIndexer && IsWrappable(property, entityMap))
                {
                    DefineIndexerProperty(property, typeBuilder, field, objectType, ignoreMappings, entityMap);
                }
                else if (proxyType == DynamicProxyType.Guard)
                {
                    DefineGuardedProperty(property, typeBuilder, field, objectType, ignoreMappings, entityMap);
                }
                else if (objectProperty != null)
                {
                    DefineProperty(property, typeBuilder, field, objectType, objectProperty);
                }
                else
                {
                    DefineDefaultProperty(property, typeBuilder);
                }
            }
        }

        private static void DefineReadOnlyProperty(string propertyName, TypeBuilder typeBuilder, FieldInfo field)
        {
            // create the new property.
            var propertyBuilder = typeBuilder.DefineProperty(propertyName, PropertyAttributes.HasDefault, field.FieldType, null);

            // The property "set" and property "get" methods require a special set of attributes.
            const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig;

            // create the getter if we can read.
            // create the get method for the property.
            var getMethodName = "get_" + propertyName;
            var getMethod = typeBuilder.DefineMethod(getMethodName, getSetAttr, field.FieldType, Type.EmptyTypes);

            // get the IL generator to generate the required IL.
            ILGenerator il = getMethod.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, field);
            il.Emit(OpCodes.Ret);

            // set the method.
            propertyBuilder.SetGetMethod(getMethod);
        }

        private static void DefineDefaultProperty(PropertyInfo property, TypeBuilder typeBuilder)
        {
            // create the new property.
            var propertyBuilder = typeBuilder.DefineProperty(property.Name, PropertyAttributes.HasDefault, property.PropertyType, null);

            // The property "set" and property "get" methods require a special set of attributes.
            //var getSetAttr = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;
            const MethodAttributes getSetAttr = MethodAttributes.Private | MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.SpecialName;

            var fieldName = "_" + property.Name;
            var currentField = typeBuilder.DefineField(fieldName, property.PropertyType, FieldAttributes.Private);

            // create the getter if we can read.
            if (property.CanRead)
            {
                // create the get method for the property.
                var getMethodName = "get_" + property.Name;
                var getMethod = typeBuilder.DefineMethod(getMethodName, getSetAttr, property.PropertyType, Type.EmptyTypes);

                // get the IL generator to generate the required IL.
                var il = getMethod.GetILGenerator();

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, currentField);
                il.Emit(OpCodes.Ret);

                // set the method.
                propertyBuilder.SetGetMethod(getMethod);
                typeBuilder.DefineMethodOverride(getMethod, property.ReflectedType.GetMethod(getMethodName));
            }

            if (property.CanWrite)
            {
                // create the set method of the property.
                var setMethodName = "set_" + property.Name;
                var setMethod = typeBuilder.DefineMethod(setMethodName, getSetAttr, null, new[] { property.PropertyType });

                // get the IL generator to generate some IL.
                var il = setMethod.GetILGenerator();

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Stfld, currentField);
                il.Emit(OpCodes.Ret);

                propertyBuilder.SetSetMethod(setMethod);
                typeBuilder.DefineMethodOverride(setMethod, property.ReflectedType.GetMethod(setMethodName));
            }
        }

        private static void DefineIndexerProperty(PropertyInfo property, TypeBuilder typeBuilder, FieldInfo field, Type objectType, bool ignoreMappings, IEntityMap entityMap)
        {
            // create the new property.
            var propertyBuilder = typeBuilder.DefineProperty(property.Name, PropertyAttributes.HasDefault, property.PropertyType, null);

            // The property "set" and property "get" methods require a special set of attributes.
            //var getSetAttr = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;
            const MethodAttributes getSetAttr = MethodAttributes.Private | MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.SpecialName;
            var columnName = MappingFactory.GetPropertyOrColumnName(property, ignoreMappings, entityMap, true);

            // create the getter if we can read.
            if (property.CanRead)
            {
                var getItemMethod = objectType.GetMethod("get_Item", new[] { typeof(string) });

                var typeConverter = MappingFactory.GetTypeConverter(getItemMethod.ReturnType, property, entityMap);

                // create the get method for the property.
                var getMethodName = "get_" + property.Name;
                var getMethod = typeBuilder.DefineMethod(getMethodName, getSetAttr, property.PropertyType, Type.EmptyTypes);

                // get the IL generator to generate the required IL.
                ILGenerator il = getMethod.GetILGenerator();

                if (typeConverter.Item1 != null)
                {	
                    //	New the converter
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }

                // load the first argument (the instance itself) and the field.
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, field);
                il.Emit(OpCodes.Ldstr, columnName);
                il.Emit(OpCodes.Callvirt, getItemMethod);
                if (typeConverter.Item1 == null)
                {
                    il.EmitCastToReference(property.PropertyType);
                }
                else
                {
                    //	Call the convert method
                    il.Emit(OpCodes.Callvirt, typeConverter.Item2.GetMethod("ConvertForward"));
                }
                il.Emit(OpCodes.Ret);

                // set the method.
                propertyBuilder.SetGetMethod(getMethod);
                typeBuilder.DefineMethodOverride(getMethod, property.ReflectedType.GetMethod(getMethodName));
            }

            // create the setter if we can read.
            if (property.CanWrite)
            {
                var setItemMethod = objectType.GetMethod("set_Item", new[] { typeof(string), typeof(object) });

                var typeConverter = MappingFactory.GetTypeConverter(setItemMethod.GetParameters()[1].ParameterType, property, entityMap);

                // create the set method of the property.
                var setMethodName = "set_" + property.Name;
                var setMethod = typeBuilder.DefineMethod(setMethodName, getSetAttr, null, new[] { property.PropertyType });

                // get the IL generator to generate some IL.
                ILGenerator il = setMethod.GetILGenerator();

                // load the first argument (instance itself) and the field.
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, field);
                il.Emit(OpCodes.Ldstr, columnName);

                if (typeConverter.Item1 != null)
                {	
                    //	New the converter
                    il.Emit(OpCodes.Newobj, typeConverter.Item1.GetConstructor(Type.EmptyTypes));
                }

                // load the second argument (holding the value).
                il.Emit(OpCodes.Ldarg_1);
                if (typeConverter.Item1 != null)
                {	//	Call the convert method
                    il.Emit(OpCodes.Callvirt, typeConverter.Item2.GetMethod("ConvertBackward"));
                }
                else
                {
                    il.BoxIfNeeded(property.PropertyType);
                }
                // directly call the inner object's get method of the property.
                il.Emit(OpCodes.Callvirt, setItemMethod);
                il.Emit(OpCodes.Ret);

                propertyBuilder.SetSetMethod(setMethod);
                typeBuilder.DefineMethodOverride(setMethod, property.ReflectedType.GetMethod(setMethodName));
            }
        }

        private static void DefineGuardedProperty(PropertyInfo property, TypeBuilder typeBuilder, FieldInfo field, Type objectType, bool ignoreMappings, IEntityMap entityMap)
        {
            // create the new property.
            var propertyBuilder = typeBuilder.DefineProperty(property.Name, System.Reflection.PropertyAttributes.HasDefault, property.PropertyType, null);

            // The property "set" and property "get" methods require a special set of attributes.
            //var getSetAttr = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;
            var getSetAttr = MethodAttributes.Private | MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.SpecialName;
            var columnName = MappingFactory.GetPropertyOrColumnName(property, ignoreMappings, entityMap, true);

            // create the getter if we can read.
            if (property.CanRead)
            {
                // create the get method for the property.
                var getMethodName = "get_" + property.Name;
                var getMethod = typeBuilder.DefineMethod(getMethodName, getSetAttr, property.PropertyType, Type.EmptyTypes);

                // get the IL generator to generate the required IL.
                ILGenerator il = getMethod.GetILGenerator();
                                               
                // directly call the inner object's get method of the property.
                Type elementType;
                if (Reflector.IsDataEntityList(property.PropertyType, out elementType))
                {
                    var isInterface = property.PropertyType.GetGenericTypeDefinition() == typeof(IList<>);
                    var asReadOnlyMethod = typeof(ObjectExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static).Where(m => m.Name == "AsReadOnly").First(m => m.GetParameters()[0].ParameterType.Name == (isInterface ? "IList`1" : "List`1"));

                    var result = il.DeclareLocal(property.PropertyType);
                    
                    // load the first argument (the instance itself) and the field.
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, field);
                    il.EmitCall(OpCodes.Callvirt, property.GetGetMethod(), null);

                    il.Emit(OpCodes.Call, asReadOnlyMethod.MakeGenericMethod(elementType));

                    il.Emit(OpCodes.Stloc_0, result);
                    il.Emit(OpCodes.Ldloc_0);
                }
                else if (Reflector.IsDataEntity(property.PropertyType))
                {
                    var asReadOnlyMethod = typeof(ObjectExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static).Where(m => m.Name == "AsReadOnly").First(m => m.GetParameters()[0].ParameterType.IsGenericParameter);

                    var result = il.DeclareLocal(property.PropertyType);

                    // load the first argument (the instance itself) and the field.
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, field);
                    il.EmitCall(OpCodes.Callvirt, property.GetGetMethod(), null);

                    il.Emit(OpCodes.Call, asReadOnlyMethod.MakeGenericMethod(property.PropertyType));

                    il.Emit(OpCodes.Stloc_0, result);
                    il.Emit(OpCodes.Ldloc_0);
                }
                else if (property.DeclaringType.InheritsFrom(typeof(ITrackableDataEntity)) && property.PropertyType == typeof(ObjectState) && property.Name == "ObjectState")
                {
                    il.Emit(OpCodes.Ldc_I4, (int)ObjectState.ReadOnly);
                }
                else
                {
                    // load the first argument (the instance itself) and the field.
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, field);
                    il.EmitCall(OpCodes.Callvirt, property.GetGetMethod(), null);
                }

                il.Emit(OpCodes.Ret);

                // set the method.
                propertyBuilder.SetGetMethod(getMethod);
                typeBuilder.DefineMethodOverride(getMethod, property.ReflectedType.GetMethod(getMethodName));
            }

            // create the setter if we can read.
            if (property.CanWrite)
            {
                // create the set method of the property.
                var setMethodName = "set_" + property.Name;
                var setMethod = typeBuilder.DefineMethod(setMethodName, getSetAttr, null, new Type[] { property.PropertyType });

                // get the IL generator to generate some IL.
                ILGenerator il = setMethod.GetILGenerator();

                // load the first argument (instance itself) and the field.
                il.Emit(OpCodes.Newobj, typeof(NotSupportedException).GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Throw);

                propertyBuilder.SetSetMethod(setMethod);
                typeBuilder.DefineMethodOverride(setMethod, property.ReflectedType.GetMethod(setMethodName));
            }
        }

        private static void DefineProperty(PropertyInfo property, TypeBuilder typeBuilder, FieldInfo field, Type objectType, PropertyInfo objectProperty)
        {
            // create the new property.
            var propertyBuilder = typeBuilder.DefineProperty(property.Name, PropertyAttributes.HasDefault, property.PropertyType, null);

            // The property "set" and property "get" methods require a special set of attributes.
            //var getSetAttr = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Virtual;
            const MethodAttributes getSetAttr = MethodAttributes.Private | MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.SpecialName;

            // create the getter if we can read.
            if (property.CanRead)
            {
                // create the get method for the property.
                var getMethodName = "get_" + property.Name;
                var getMethod = typeBuilder.DefineMethod(getMethodName, getSetAttr, property.PropertyType, Type.EmptyTypes);

                // get the IL generator to generate the required IL.
                ILGenerator il = getMethod.GetILGenerator();

                // load the first argument (the instance itself) and the field.
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, field);

                // check if the inner type is public and in the same assembly.
                if (!objectType.IsPublic)
                {
                    // required to create some reflection code to access internal and outside of the
                    // assembly properties.
                    il.EmitCall(OpCodes.Callvirt, objectType.GetMethod("GetType"), null);

                    // get the property.
                    il.Emit(OpCodes.Ldstr, objectProperty.Name);
                    il.EmitCall(OpCodes.Callvirt, typeof(Type).GetMethod("GetProperty", new[] { typeof(string) }), null);

                    // load the arguments for the next method call.
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, field);
                    il.Emit(OpCodes.Ldnull);
                    il.EmitCall(OpCodes.Callvirt, typeof(PropertyInfo).GetMethod("GetValue", new[] { typeof(object), typeof(object[]) }), null);

                    // cast or unbox if required.
                    il.EmitCastToReference(property.PropertyType);
                }
                else
                {
                    // directly call the inner object's get method of the property.
                    il.EmitCall(OpCodes.Callvirt, objectProperty.GetGetMethod(), null);
                }

                il.Emit(OpCodes.Ret);

                // set the method.
                propertyBuilder.SetGetMethod(getMethod);
                typeBuilder.DefineMethodOverride(getMethod, property.ReflectedType.GetMethod(getMethodName));
            }

            // create the setter if we can read.
            if (property.CanWrite)
            {
                // create the set method of the property.
                var setMethodName = "set_" + property.Name;
                var setMethod = typeBuilder.DefineMethod(setMethodName, getSetAttr, null, new[] { property.PropertyType });

                // get the IL generator to generate some IL.
                ILGenerator il = setMethod.GetILGenerator();

                // load the first argument (instance itself) and the field.
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, field);

                // check if the inner type is public and in the same assembly.
                if (!objectType.IsPublic)
                {
                    // required to create some reflection code to access internal and outside of the
                    // assembly properties.
                    il.EmitCall(OpCodes.Callvirt, objectType.GetMethod("GetType"), null);

                    // get the property.
                    il.Emit(OpCodes.Ldstr, objectProperty.Name);
                    il.EmitCall(OpCodes.Callvirt, typeof(Type).GetMethod("GetProperty", new[] { typeof(string) }), null);

                    // load the various arguments and items on the stack.
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, field);
                    il.Emit(OpCodes.Ldarg_1);
                    // box if a value type.
                    il.BoxIfNeeded(property.PropertyType);
                    il.Emit(OpCodes.Ldnull);

                    // set the value for the property.
                    il.EmitCall(OpCodes.Callvirt, typeof(PropertyInfo).GetMethod("SetValue", new[] { typeof(object), typeof(object), typeof(object[]) }), null);

                    // check if we need to unbox or cast.
                    il.EmitCastToReference(property.PropertyType);
                }
                else
                {
                    // load the second argument (holding the value).
                    il.Emit(OpCodes.Ldarg_1);
                    // directly call the inner object's get method of the property.
                    il.EmitCall(OpCodes.Callvirt, objectProperty.GetSetMethod(), null);
                }
                il.Emit(OpCodes.Ret);

                propertyBuilder.SetSetMethod(setMethod);
                typeBuilder.DefineMethodOverride(setMethod, property.ReflectedType.GetMethod(setMethodName));
            }
        }

        private static bool IsWrappable(PropertyInfo property, IEntityMap entityMap)
        {
            var propertyType = property.PropertyType;
            return Reflector.IsSimpleType(propertyType) 
                || propertyType == typeof(byte[])
                || (Reflector.IsSimpleList(propertyType) && HasListTypeConverter(property, entityMap))
                || (Reflector.IsXmlType(propertyType) && HasXmlTypeConverter(property, entityMap));
        }

        private static bool HasXmlTypeConverter(PropertyInfo property, IEntityMap entityMap)
        {
            var result = false;
            
            if (entityMap != null)
            {
                var propertyMap = entityMap.Properties.FirstOrDefault(p => p.Property.PropertyName == property.Name);
                if (propertyMap != null && propertyMap.Property.Converter != null)
                {
                    result = propertyMap.Property.Converter == typeof(XmlTypeConverter) || propertyMap.Property.Converter == typeof(XmlReaderTypeConverter);
                }
            }

            if (result) return true;

            var customAttributes = property.GetCustomAttributes(typeof(TypeConverterAttribute), false).Cast<TypeConverterAttribute>();
            foreach (var attribute in customAttributes)
            {
                result = attribute.TypeConverterType == typeof(XmlTypeConverter) || attribute.TypeConverterType == typeof(XmlReaderTypeConverter);
                if (result) break;
            }
            return result;
        }

        private static bool HasListTypeConverter(PropertyInfo property, IEntityMap entityMap)
        {
            bool result = false;

            if (entityMap != null)
            {
                var propertyMap = entityMap.Properties.FirstOrDefault(p => p.Property.PropertyName == property.Name);
                if (propertyMap != null && propertyMap.Property.Converter != null)
                {
                    result = propertyMap.Property.Converter.GetGenericTypeDefinition() == typeof(ListConverter<>);
                }
            }

            if (result) return true;

            var customAttributes = property.GetCustomAttributes(typeof(TypeConverterAttribute), false).Cast<TypeConverterAttribute>();
            foreach (var attribute in customAttributes)
            {
                result = attribute.TypeConverterType.IsGenericType && attribute.TypeConverterType.GetGenericTypeDefinition() == typeof(ListConverter<>);
                if (result) break;
            }
            return result;
        }

        private static void DefineConstructor(Type objectType, TypeBuilder typeBuilder, FieldInfo field)
        {
            var constructor = typeBuilder.DefineConstructor(MethodAttributes.Public | MethodAttributes.HideBySig, CallingConventions.Standard, new[] { objectType });

            // create the constructor.
            var constructorIL = constructor.GetILGenerator();
            // call the base constructor. in this case of object.
            constructorIL.Emit(OpCodes.Ldarg_0);
            constructorIL.Emit(OpCodes.Call, typeof(object).GetConstructor(Type.EmptyTypes));
            // load the instance, then the object that is passed in and set
            // the field of the instance to the object.
            constructorIL.Emit(OpCodes.Ldarg_0);
            constructorIL.Emit(OpCodes.Ldarg_1);
            constructorIL.Emit(OpCodes.Stfld, field);
            constructorIL.Emit(OpCodes.Ret);
        }
    }
}
