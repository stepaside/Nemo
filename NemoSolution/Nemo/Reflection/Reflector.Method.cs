using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;

namespace Nemo.Reflection
{
    public static partial class Reflector
    {
        public static class Method
        {
            public delegate object FastInvoker(object target, object[] paramters);

            private static readonly ConcurrentDictionary<RuntimeMethodHandle, FastInvoker> _invokers = new ConcurrentDictionary<RuntimeMethodHandle, FastInvoker>();

            public static FastInvoker CreateDelegate(RuntimeMethodHandle methodHandle)
            {
                FastInvoker invoker = _invokers.GetOrAdd(methodHandle, GenerateDelegate);
                return invoker;
            }

            private static FastInvoker GenerateDelegate(RuntimeMethodHandle methodHandle)
            {
                var methodInfo = (MethodInfo)MethodInfo.GetMethodFromHandle(methodHandle);
                var dynamicMethod = new DynamicMethod(string.Empty, typeof(object), new[] { typeof(object), typeof(object[]) }, methodInfo.DeclaringType.Module);
                var il = dynamicMethod.GetILGenerator();            
                var ps = methodInfo.GetParameters();
                var paramTypes = new Type[ps.Length];
                for (int i = 0; i < paramTypes.Length; i++)
                {
                    if (ps[i].ParameterType.IsByRef)
                    {
                        paramTypes[i] = ps[i].ParameterType.GetElementType();
                    }
                    else
                    {
                        paramTypes[i] = ps[i].ParameterType;
                    }
                }

                var locals = new LocalBuilder[paramTypes.Length];
                for (var i = 0; i < paramTypes.Length; i++)
                {
                    locals[i] = il.DeclareLocal(paramTypes[i], true);
                }

                for (var i = 0; i < paramTypes.Length; i++)
                {
                    il.Emit(OpCodes.Ldarg_1);
                    il.EmitFastInt(i);
                    il.Emit(OpCodes.Ldelem_Ref);
                    il.EmitCastToReference(paramTypes[i]);
                    il.Emit(OpCodes.Stloc, locals[i]);
                }

                if (!methodInfo.IsStatic)
                {
                    il.Emit(OpCodes.Ldarg_0);
                }

                for (var i = 0; i < paramTypes.Length; i++)
                {
                    il.Emit(ps[i].ParameterType.IsByRef ? OpCodes.Ldloca_S : OpCodes.Ldloc, locals[i]);
                }

                il.EmitCall(methodInfo.IsStatic ? OpCodes.Call : OpCodes.Callvirt, methodInfo, null);

                if (methodInfo.ReturnType == typeof(void))
                {
                    il.Emit(OpCodes.Ldnull);
                }
                else
                {
                    il.BoxIfNeeded(methodInfo.ReturnType);
                }

                il.Emit(OpCodes.Ret);
                var invoker = (FastInvoker)dynamicMethod.CreateDelegate(typeof(FastInvoker));
                return invoker;
            }
        }
    }
}
