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

        private static readonly MethodInfo CountMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "Count" && m.GetGenericArguments().Length == 2);
        private static readonly MethodInfo CountAsyncMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "CountAsync" && m.GetGenericArguments().Length == 2);
        
        private static readonly MethodInfo AggregateMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "Aggregate" && m.GetGenericArguments().Length == 2);
        private static readonly MethodInfo AggregateAsyncMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "AggregateAsync" && m.GetGenericArguments().Length == 2);


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
            LambdaExpression projection = null;
            var orderBy = new List<ISorting>();

            var count = false;
            var longCount = false;
            var aggregate = false;
            var aggregateName = string.Empty;

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
                    case "Count":
                    case "LongCount":
                    case "CountAsync":
                    case "LongCountAsync":
                        criteria = pair.Value as LambdaExpression;
                        count = true;
                        longCount = pair.Key.StartsWith("Long");
                        break;
                    case "Max":
                    case "Min":
                    case "Average":
                    case "Sum":
                    case "MaxAsync":
                    case "MinAsync":
                    case "AverageAsync":
                    case "SumAsync":
                        projection = pair.Value as LambdaExpression;
                        aggregate = true;
                        aggregateName = pair.Key;
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

            if (count)
            {
                return (async ? CountAsyncMethod : CountMethod).MakeGenericMethod(type, longCount ? typeof(long) : typeof(int))
                    .Invoke(null, new object[] { criteria, null, connection });
            }

            if (aggregate)
            {
                var property = (PropertyInfo)(projection.Body as MemberExpression).Member;
                return (async ? AggregateAsyncMethod : AggregateMethod).MakeGenericMethod(type, property.PropertyType)
                   .Invoke(null, new object[] { Enum.Parse(typeof(ObjectFactory.AggregateNames), aggregateName.Replace("Async", ""), true), projection, criteria, null, connection });
            }

            return (async ? SelectAsyncMethod : SelectMethod).MakeGenericMethod(type)
                .Invoke(null, new object[] { criteria, null, connection, limit > 0 ? offset / limit + 1 : 0, limit, offset, null, selectOption, orderByArray });
        }

        private readonly static ISet<string> SupportedConsumeMethods = new HashSet<string>(new[] { "First", "FirstOrDefault" });
        private readonly static ISet<string> SupportedAggregateMethods = new HashSet<string>(new[] { "Count", "LongCount", "Max", "Min", "Average", "Sum" });
        private readonly static ISet<string> SupportedConsumeMethodsAsync = new HashSet<string>(new[] { "FirstAsync", "FirstOrDefaultAsync" });
        private readonly static ISet<string> SupportedAggregateMethodsAsync = new HashSet<string>(new[] { "CountAsync", "LongCountAsync", "MaxAsync", "MinAsync", "AverageAsync", "SumAsync" });

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
                    var lambda = (LambdaExpression)((UnaryExpression)(methodCall.Arguments[1])).Operand;
                    if (type == null && ((!async && SupportedConsumeMethods.Contains(method)) || (async && SupportedConsumeMethodsAsync.Contains(method))))
                    {
                        type = returnType;
                    }
                    else if (type == null && ((!async && SupportedAggregateMethods.Contains(method)) || (async && SupportedAggregateMethodsAsync.Contains(method))))
                    {
                        type = lambda.Parameters[0].Type;
                    }                   
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
                    else if ((!async && SupportedConsumeMethods.Contains(method)) || (async && SupportedConsumeMethodsAsync.Contains(method)))
                    {
                        args[method] = null;
                        if (async && type == null)
                        {
                            type = returnType;
                        }
                    }
                    else if ((!async && SupportedAggregateMethods.Contains(method)) || (async && SupportedAggregateMethodsAsync.Contains(method)))
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