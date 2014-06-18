using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nemo.Collections.Extensions;
using Nemo.Reflection;

namespace Nemo.Linq
{
    class NemoQueryContext
    {
        // Executes the expression tree that is passed to it. 
        internal static object Execute(Expression expression)
        {
            var args = new Dictionary<string, object>();
            var type = Prepare(expression, args);

            var funcType = typeof(Func<,>).MakeGenericType(type, typeof(object));
            var expressinoType = typeof(Expression<>).MakeGenericType(funcType);
            var tupleType = typeof(Tuple<,>).MakeGenericType(expressinoType, typeof(SortingOrder));
            
            var offset=0; 
            var limit=0;
            LambdaExpression criteria = null;
            var orderBy = new List<Tuple<LambdaExpression, SortingOrder>>();

            foreach (var pair in args)
            {
                switch (pair.Key)
                {
                    case "Take":
                        limit = (int)pair.Value;
                        break;
                    case "Limit":
                        offset = (int)pair.Value;
                        break;
                    case "Where":
                        criteria = (LambdaExpression)pair.Value;
                        break;
                    default:
                        if (pair.Key.StartsWith("OrderBy."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            orderBy.Add(Tuple.Create(Expression.Lambda(funcType, exp.Body, exp.Parameters), SortingOrder.Ascending));
                        }
                        else if (pair.Key.StartsWith("OrderByDescending."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            orderBy.Add(Tuple.Create(Expression.Lambda(funcType, exp.Body, exp.Parameters), SortingOrder.Descending));
                        }
                        else if (pair.Key.StartsWith("ThenBy."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            orderBy.Add(Tuple.Create(Expression.Lambda(funcType, exp.Body, exp.Parameters), SortingOrder.Ascending));
                        }
                        else if (pair.Key.StartsWith("ThenByDescending."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            orderBy.Add(Tuple.Create(Expression.Lambda(funcType, exp.Body, exp.Parameters), SortingOrder.Descending));
                        }
                        break;
                }
            }

            var orderByArray = Array.CreateInstance(tupleType, orderBy.Count);
            for (var i = 0; i < orderBy.Count; i++)
            {
                var t = orderBy[i];
                orderByArray.SetValue(tupleType.New(new object[] { t.Item1, t.Item2 }), i);
            }

            return typeof(ObjectFactory).GetMethods().First(m => m.Name == "Select" && m.GetGenericArguments().Length == 1)
                .MakeGenericMethod(type)
                .Invoke(null, new object[] { criteria, null, null, limit > 0 ? offset/limit + 1 : 0, limit, null, orderByArray });
        }

        private static Type Prepare(Expression expression, IDictionary<string, object> args)
        {
            Type type = null;
            while (true)
            {
                // The expression must represent a query over the data source. 
                if (!IsQueryOverDataSource(expression))
                {
                    throw new InvalidProgramException("Invalid query specified.");
                }
                
                var methodCall = (MethodCallExpression)expression;

                if (type == null)
                {
                    type = Reflector.GetElementType(methodCall.Method.ReturnType);
                }

                var method = methodCall.Method.Name;
                if (methodCall.Arguments[0].NodeType == ExpressionType.Constant)
                {
                    var lambda = (LambdaExpression)((UnaryExpression)(methodCall.Arguments[1])).Operand;
                    args[method] = lambda;
                }
                else
                {
                    var exp = methodCall.Arguments[1];
                    var unaryExpression = exp as UnaryExpression;
                    if (unaryExpression != null)
                    {
                        var lambda = (LambdaExpression)unaryExpression.Operand;
                        if (method.StartsWith("OrderBy") || method.StartsWith("ThenBy"))
                        {
                            var count = args.Keys.Count(k => k == method);
                            args[method + "." + count] = lambda;
                        }
                        else
                        {
                            args[method] = lambda;
                        }
                    }
                    else
                    {
                        var constantExpression = exp as ConstantExpression;
                        if (constantExpression != null)
                        {
                            var value = constantExpression.Value;
                            args[method] = value;
                        }
                    }
                    expression = methodCall.Arguments[0];
                    continue;
                }

                break;
            }
            return type;
        }

        private static bool IsQueryOverDataSource(Expression expression)
        {
            // If expression represents an unqueried IQueryable data source instance, 
            // expression is of type ConstantExpression, not MethodCallExpression. 
            return (expression is MethodCallExpression);
        }
    }
}