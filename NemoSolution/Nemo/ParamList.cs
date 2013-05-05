using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Nemo
{
    public class ParamList : List<Expression<Func<object, object>>>
    {
        private static ConcurrentDictionary<Tuple<Type, string, string, Type>, Func<object, object>> _parameterCache = new ConcurrentDictionary<Tuple<Type, string, string, Type>, Func<object, object>>();

        internal Param[] ExtractParameters(Type objectType, string operation)
        {
            var result = new Param[this.Count];
            for (int i = 0; i < this.Count; ++i)
            {
                var expression = this[i];
                var parameterName = expression.Parameters[0].Name;
                var returnType = expression.Body.Type;
                var key = Tuple.Create(objectType, operation, parameterName, returnType);
                Func<Tuple<Type, string, string, Type>, Func<object, object>> valueFactory = t => expression.Compile();
                var func = _parameterCache.GetOrAdd(key, valueFactory);

                var parameter = new Param { Name = parameterName };

                if (returnType == typeof(Param))
                {
                    parameter = (Param)func(null);
                    if (parameter.Name == null)
                    {
                        parameter.Name = parameterName;
                    }
                }
                else
                {
                    parameter.Value = func(null);
                }

                result[i] = parameter;
            }
            return result;
        }
    }

}
