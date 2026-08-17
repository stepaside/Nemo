using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Nemo.Configuration;

namespace Nemo.Linq
{
    internal class NemoQueryContext
    {
        private static readonly MethodInfo SelectMethod = typeof(ObjectFactory).GetMethods().First(m => m.Name == "Select" && m.GetGenericArguments().Length == 1);
        private static readonly MethodInfo SelectAsyncMethod = typeof(ObjectFactory).GetMethods().First(m => m.Name == "SelectAsync" && m.GetGenericArguments().Length == 1);

        private static readonly MethodInfo CountMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "Count" && m.GetGenericArguments().Length == 2);
        private static readonly MethodInfo CountAsyncMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "CountAsync" && m.GetGenericArguments().Length == 2);
        
        private static readonly MethodInfo SortPageMethod = typeof(NemoQueryContext).GetMethod(nameof(SortPage), BindingFlags.NonPublic | BindingFlags.Static);
        private static readonly MethodInfo SortPageAsyncMethod = typeof(NemoQueryContext).GetMethod(nameof(SortPageAsync), BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly MethodInfo EmptyMethod = typeof(NemoQueryContext).GetMethod(nameof(Empty), BindingFlags.NonPublic | BindingFlags.Static);
        private static readonly MethodInfo EmptyAsyncMethod = typeof(NemoQueryContext).GetMethod(nameof(EmptyAsync), BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly MethodInfo AggregateMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "Aggregate" && m.GetGenericArguments().Length == 2);
        private static readonly MethodInfo AggregateAsyncMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "AggregateAsync" && m.GetGenericArguments().Length == 2);

        private static readonly ConcurrentDictionary<(MethodInfo Method, Type Type1, Type Type2), Func<object[], object>> Invokers = new ConcurrentDictionary<(MethodInfo, Type, Type), Func<object[], object>>();

        private static readonly ConcurrentDictionary<Type, (Type SortingType, Type FuncType, Func<ISorting> Factory)> SortingTypes = new ConcurrentDictionary<Type, (Type, Type, Func<ISorting>)>();

        private static readonly ConditionalWeakTable<Expression, NemoQueryPlan> SyncPlans = new ConditionalWeakTable<Expression, NemoQueryPlan>();

        private static readonly ConditionalWeakTable<Expression, NemoQueryPlan> AsyncPlans = new ConditionalWeakTable<Expression, NemoQueryPlan>();

        private static Func<object[], object> GetOrAddInvoker(MethodInfo method, Type type1, Type type2 = null)
        {
            return Invokers.GetOrAdd((method, type1, type2), key =>
            {
                var closedMethod = key.Type2 != null ? key.Method.MakeGenericMethod(key.Type1, key.Type2) : key.Method.MakeGenericMethod(key.Type1);
                var args = Expression.Parameter(typeof(object[]), "args");
                var parameters = closedMethod.GetParameters();
                var callArgs = new Expression[parameters.Length];
                for (var i = 0; i < parameters.Length; i++)
                {
                    callArgs[i] = Expression.Convert(Expression.ArrayIndex(args, Expression.Constant(i)), parameters[i].ParameterType);
                }
                var body = Expression.Convert(Expression.Call(closedMethod, callArgs), typeof(object));
                return Expression.Lambda<Func<object[], object>>(body, args).Compile();
            });
        }

        // Executes the expression tree that is passed to it. 
        internal static object Execute(Expression expression, DbConnection connection = null, bool async = false, INemoConfiguration config = null, CancellationToken cancellationToken = default)
        {
            var plan = async
                ? AsyncPlans.GetValue(expression, e => NemoQueryParser.Parse(e, true))
                : SyncPlans.GetValue(expression, e => NemoQueryParser.Parse(e, false));
            var type = plan.ElementType;

            if (plan.IsEmpty && !plan.IsCount && plan.Aggregate == null)
            {
                return GetOrAddInvoker(async ? EmptyAsyncMethod : EmptyMethod, type)(Array.Empty<object>());
            }

            if (plan.IsCount)
            {
                var countArgs = async
                    ? new object[] { plan.Predicate, null, connection, config, cancellationToken }
                    : new object[] { plan.Predicate, null, connection, config };
                return GetOrAddInvoker(async ? CountAsyncMethod : CountMethod, type, plan.IsLongCount ? typeof(long) : typeof(int))(countArgs);
            }

            if (plan.Aggregate != null)
            {
                var aggregateArgs = async
                    ? new object[] { plan.Aggregate.Value, plan.AggregateProjection, plan.Predicate, null, connection, config, cancellationToken }
                    : new object[] { plan.Aggregate.Value, plan.AggregateProjection, plan.Predicate, null, connection, config };
                return GetOrAddInvoker(async ? AggregateAsyncMethod : AggregateMethod, type, plan.AggregateProperty.PropertyType)(aggregateArgs);
            }

            plan.GetPaging(out var page, out var pageSize, out var skipCount);

            var (sortingType, funcType, sortingFactory) = SortingTypes.GetOrAdd(type, t =>
            {
                var st = typeof(Sorting<>).MakeGenericType(t);
                var factory = Expression.Lambda<Func<ISorting>>(Expression.New(st)).Compile();
                return (st, typeof(Func<,>).MakeGenericType(t, typeof(object)), factory);
            });
            var orderByArray = Array.CreateInstance(sortingType, plan.OrderBy.Count);
            for (var i = 0; i < plan.OrderBy.Count; i++)
            {
                var sort = plan.OrderBy[i];
                var sorting = sortingFactory();
                var body = sort.KeySelector.Body;
                if (body.Type.IsValueType)
                {
                    body = Expression.Convert(body, typeof(object));
                }
                sorting.SetOrderBy(Expression.Lambda(funcType, body, sort.KeySelector.Parameters));
                sorting.Reverse = sort.Descending;
                orderByArray.SetValue(sorting, i);
            }

            var selectOption = plan.PostOrderBy.Count > 0 && plan.SelectOption != SelectOption.All ? SelectOption.All : plan.SelectOption;
            var result = GetOrAddInvoker(async ? SelectAsyncMethod : SelectMethod, type)(new object[] { plan.Predicate, null, connection, page, pageSize, skipCount, null, selectOption, config, orderByArray });

            if (plan.PostOrderBy.Count > 0)
            {
                return GetOrAddInvoker(async ? SortPageAsyncMethod : SortPageMethod, type)(new object[] { result, plan, cancellationToken });
            }

            return result;
        }

        private static Func<T, object> GetKeySelector<T>(NemoQuerySort sort)
        {
            if (sort.CompiledKeySelector is Func<T, object> compiled)
            {
                return compiled;
            }
            var body = sort.KeySelector.Body;
            if (body.Type.IsValueType)
            {
                body = Expression.Convert(body, typeof(object));
            }
            var selector = Expression.Lambda<Func<T, object>>(body, sort.KeySelector.Parameters).Compile();
            sort.CompiledKeySelector = selector;
            return selector;
        }

        private static IEnumerable<T> ApplySort<T>(IEnumerable<T> items, NemoQueryPlan plan)
        {
            IOrderedEnumerable<T> ordered = null;
            foreach (var sort in plan.PostOrderBy)
            {
                var keySelector = GetKeySelector<T>(sort);
                if (ordered == null)
                {
                    ordered = sort.Descending ? items.OrderByDescending(keySelector) : items.OrderBy(keySelector);
                }
                else
                {
                    ordered = sort.Descending ? ordered.ThenByDescending(keySelector) : ordered.ThenBy(keySelector);
                }
            }
            return ordered ?? items;
        }

        private static IEnumerable<T> SortPage<T>(IEnumerable<T> source, NemoQueryPlan plan, CancellationToken cancellationToken)
        {
            return ApplySort(source, plan);
        }

        private static async IAsyncEnumerable<T> SortPageAsync<T>(IAsyncEnumerable<T> source, NemoQueryPlan plan, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var items = new List<T>();
            await foreach (var item in System.Threading.Tasks.TaskAsyncEnumerableExtensions.WithCancellation(source, cancellationToken).ConfigureAwait(false))
            {
                items.Add(item);
            }

            foreach (var item in ApplySort(items, plan))
            {
                yield return item;
            }
        }

        private static IEnumerable<T> Empty<T>()
        {
            return Enumerable.Empty<T>();
        }

        private static async IAsyncEnumerable<T> EmptyAsync<T>()
        {
            await System.Threading.Tasks.Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }
    }
}
