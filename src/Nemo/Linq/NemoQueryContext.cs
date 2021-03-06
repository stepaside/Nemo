using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections.Extensions;
using Nemo.Reflection;

namespace Nemo.Linq
{
    internal class NemoQueryContext
    {
        private static readonly MethodInfo SelectMethod = typeof(ObjectFactory).GetMethods().First(m => m.Name == "Select" && m.GetGenericArguments().Length == 1);
        private static readonly MethodInfo SelectAsyncMethod = typeof(ObjectFactory).GetMethods().First(m => m.Name == "SelectAsync" && m.GetGenericArguments().Length == 1);

        // Executes the expression tree that is passed to it. 
        internal static object Execute(Expression expression, DbConnection connection = null, bool async = false)
        {
            var args = new Dictionary<string, object>();
            var type = Prepare(expression, args, async);

            var funcType = typeof(Func<,>).MakeGenericType(type, typeof(object));
            var sortingType = typeof(Sorting<>).MakeGenericType(type);

            var offset = 0;
            var limit = 0;
            var selectOption = SelectOption.All;
            LambdaExpression criteria = null;
            var orderBy = new List<ISorting>();

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
                    case "First":
                    case "FirstOrDefault":
                    case "FirstAsync":
                    case "FirstOrDefaultAsync":
                        selectOption = pair.Key == "First" || pair.Key == "FirstAsync" ? SelectOption.First : SelectOption.FirstOrDefault;
                        criteria = pair.Value as LambdaExpression;
                        break;
                    default:
                        if (pair.Key.StartsWith("OrderBy."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            var sorting = (ISorting)System.Activator.CreateInstance(sortingType);
                            sorting.SetOrderBy(Expression.Lambda(funcType, exp.Body, exp.Parameters));
                            orderBy.Add(sorting);
                        }
                        else if (pair.Key.StartsWith("OrderByDescending."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            var sorting = (ISorting)System.Activator.CreateInstance(sortingType);
                            sorting.SetOrderBy(Expression.Lambda(funcType, exp.Body, exp.Parameters));
                            sorting.Reverse = true;
                            orderBy.Add(sorting);
                        }
                        else if (pair.Key.StartsWith("ThenBy."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            var sorting = (ISorting)System.Activator.CreateInstance(sortingType);
                            sorting.SetOrderBy(Expression.Lambda(funcType, exp.Body, exp.Parameters));
                            orderBy.Add(sorting);
                        }
                        else if (pair.Key.StartsWith("ThenByDescending."))
                        {
                            var exp = (LambdaExpression)pair.Value;
                            var sorting = (ISorting)System.Activator.CreateInstance(sortingType);
                            sorting.SetOrderBy(Expression.Lambda(funcType, exp.Body, exp.Parameters));
                            sorting.Reverse = true;
                            orderBy.Add(sorting);
                        }
                        break;
                }
            }

            var orderByArray = Array.CreateInstance(sortingType, orderBy.Count);
            for (var i = 0; i < orderBy.Count; i++)
            {
                var t = orderBy[i];
                orderByArray.SetValue(t, i);
            }

            return (async ? SelectAsyncMethod : SelectMethod).MakeGenericMethod(type)
                .Invoke(null, new object[] { criteria, null, connection, limit > 0 ? offset / limit + 1 : 0, limit, offset, null, selectOption, orderByArray });
        }

        private static Type Prepare(Expression expression, IDictionary<string, object> args, bool async)
        {
            Type type = null;
            while (true)
            {
                // The expression must represent a query over the data source. 
                if (!IsQueryOverDataSource(expression, out var queryable))
                {
                    throw new InvalidProgramException("Invalid query specified.");
                }

                if (queryable != null)
                {
                    return queryable.ElementType;
                }

                var methodCall = (MethodCallExpression)expression;

                Type returnType = methodCall.Method.ReturnType;

                if (async && returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
                {
                    returnType = returnType.GetGenericArguments()[0];
                }

                if (type == null)
                {
                    type = Reflector.GetElementType(returnType);
                }

                var method = methodCall.Method.Name;
                if (methodCall.Arguments[0].NodeType == ExpressionType.Constant)
                {
                    if (type == null && ((!async && method == "First") || (!async && method == "FirstOrDefault") || (async && method == "FirstAsync") || (async && method == "FirstOrDefaultAsync")))
                    {
                        type = returnType;
                    }
                    var lambda = (LambdaExpression)((UnaryExpression)(methodCall.Arguments[1])).Operand;
                    args[method] = lambda;
                }
                else
                {
                    var exp = methodCall.Arguments.ElementAtOrDefault(1);
                    if (exp != null)
                    {
                        var unaryExpression = exp as UnaryExpression;
                        if (unaryExpression != null)
                        {
                            var lambda = (LambdaExpression)unaryExpression.Operand;
                            if (method.StartsWith("OrderBy") || method.StartsWith("ThenBy"))
                            {
                                var count = args.Keys.Count(k => k == method);
                                args[method + "." + count] = lambda;
                            }
                            else if (method != "First" && method != "FirstOrDefault" && method != "FirstAsync" && method != "FirstOrDefaultAsync")
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

                                if (value is CancellationToken && (async && method == "FirstAsync") || (async && method == "FirstOrDefaultAsync"))
                                {
                                    if (async && type == null)
                                    {
                                        type = returnType;
                                    }
                                }
                            }
                        }
                    }
                    else if ((!async && method == "First") || (!async && method == "FirstOrDefault") || (async && method == "FirstAsync") || (async && method == "FirstOrDefaultAsync"))
                    {
                        args[method] = null;
                        if (async && type == null)
                        {
                            type = returnType;
                        }
                    }
                    expression = methodCall.Arguments[0];
                    continue;
                }

                break;
            }
            return type;
        }

        private static bool IsQueryOverDataSource(Expression expression, out IQueryable queryable)
        {
            // If expression represents an unqueried IQueryable data source instance, 
            // expression is of type ConstantExpression, not MethodCallExpression. 
            queryable = null;
            if (expression is MethodCallExpression) return true;
                
            if (expression is ConstantExpression constantExpression && constantExpression.Value is IQueryable query)
            {
                queryable = query;
                return true;
            }

            return false;
        }
    }
}