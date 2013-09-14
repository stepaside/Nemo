using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Nemo
{
    public class ParamList : List<Expression<Func<object, object>>>
    {
        internal Param[] ExtractParameters(Type objectType, string operation)
        {
            var result = new Param[this.Count];
            for (int i = 0; i < this.Count; ++i)
            {
                var expression = this[i];
                var parameterName = expression.Parameters[0].Name;
                var parameterValue = expression.Body is ConstantExpression ? ((ConstantExpression)expression.Body).Value : expression.Compile()(null);
                
                var parameter = new Param { Name = parameterName, Value = parameterValue };

                result[i] = parameter;
            }
            return result;
        }
    }

}
