using Nemo.Collections.Extensions;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Nemo.Utilities;

namespace Nemo.Reflection
{
    public static class Activator
    {
        private static readonly ConcurrentDictionary<uint, ObjectActivator> _activatorCache = new ConcurrentDictionary<uint, ObjectActivator>();

        public delegate object ObjectActivator(params object[] args);

        public static object New(this Type type, params object[] args)
        {
            var activator = CreateDelegate(type, args.Select(t => t.GetType()).ToArray());
            return activator(args);
        }

        internal static ObjectActivator CreateDelegate(Type type, params Type[] types)
        {
            var count = 1 + types.Length;
            var data = new byte[sizeof(int) * count];
            type.Prepend(types)
                .Select(t => BitConverter.GetBytes(t.GetHashCode()))
                .Run((i, b) => Buffer.BlockCopy(b, 0, data, i * sizeof(int), b.Length));
            var key = Hash.Compute(data);
            return _activatorCache.GetOrAdd(key, (Func<uint, ObjectActivator>)(k => GenerateDelegate(type, types)));
        }

        private static ObjectActivator GenerateDelegate(Type type, params Type[] types)
        {
            var ctors = type.GetConstructors();

            ConstructorInfo ctor = null;
            ParameterInfo[] paramsInfo = null;

            foreach (var c in ctors)
            {
                var p = c.GetParameters();
                
                if (p.Length != types.Length) continue;

                if (p.Length == 0)
                {
                    ctor = c;
                    paramsInfo = p;
                    break;
                }
                var count = p.Select(a => a.ParameterType).Zip(types, (t1, t2) => t1 == t2 || t1.IsAssignableFrom(t2) || t2.IsAssignableFrom(t1) ? 1 : 0).Sum();
                
                if (count != types.Length) continue;

                ctor = c;
                paramsInfo = p;
                break;
            }

            var method = new DynamicMethod("CreateInstance", type, new[] { typeof(object[]) }, true); // skip visibility is on to allow instantiation of anonyopus type wrappers
            var il = method.GetILGenerator();
            for (var i = 0; i < paramsInfo.Length; i++)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, i);
                il.Emit(OpCodes.Ldelem_Ref);
                il.EmitCastToReference(paramsInfo[i].ParameterType);
            }

            il.Emit(OpCodes.Newobj, ctor);
            il.Emit(OpCodes.Ret);

            var activator = (ObjectActivator)method.CreateDelegate(typeof(ObjectActivator));

            ////create a single param of type object[]
            //ParameterExpression param = Expression.Parameter(typeof(object[]), "args");

            //Expression[] argsExp = new Expression[paramsInfo.Length];

            ////pick each arg from the params array 
            ////and create a typed expression of them
            //for (int i = 0; i < paramsInfo.Length; i++)
            //{
            //    Expression index = Expression.Constant(i);
            //    Type paramType = paramsInfo[i].ParameterType;

            //    Expression paramAccessorExp = Expression.ArrayIndex(param, index);

            //    Expression paramCastExp = Expression.Convert(paramAccessorExp, paramType);

            //    argsExp[i] = paramCastExp;
            //}

            ////make a NewExpression that calls the
            ////ctor with the args we just created
            //NewExpression newExp = Expression.New(ctor, argsExp);

            ////create a lambda with the New
            ////Expression as body and our param object[] as arg
            //LambdaExpression lambda = Expression.Lambda(typeof(ObjectActivator), newExp, param);

            //var activator = (ObjectActivator)lambda.Compile();
            return activator;
        }
    }
}
