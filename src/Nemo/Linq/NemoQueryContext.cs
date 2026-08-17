using System;
using System.Collections.Concurrent;
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
        
        private static readonly MethodInfo AggregateMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "Aggregate" && m.GetGenericArguments().Length == 2);
        private static readonly MethodInfo AggregateAsyncMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).First(m => m.Name == "AggregateAsync" && m.GetGenericArguments().Length == 2);

        private static readonly ConcurrentDictionary<(MethodInfo Method, Type Type1, Type Type2), MethodInfo> ClosedGenericMethods = new ConcurrentDictionary<(MethodInfo, Type, Type), MethodInfo>();

        private static readonly ConcurrentDictionary<Type, (Type SortingType, Type FuncType)> SortingTypes = new ConcurrentDictionary<Type, (Type, Type)>();

        private static readonly ConditionalWeakTable<Expression, NemoQueryPlan> SyncPlans = new ConditionalWeakTable<Expression, NemoQueryPlan>();

        private static readonly ConditionalWeakTable<Expression, NemoQueryPlan> AsyncPlans = new ConditionalWeakTable<Expression, NemoQueryPlan>();

        private static MethodInfo GetOrAddClosedMethod(MethodInfo method, Type type1, Type type2 = null)
        {
            return ClosedGenericMethods.GetOrAdd((method, type1, type2), key => key.Type2 != null ? key.Method.MakeGenericMethod(key.Type1, key.Type2) : key.Method.MakeGenericMethod(key.Type1));
        }

        // Executes the expression tree that is passed to it. 
        internal static object Execute(Expression expression, DbConnection connection = null, bool async = false, INemoConfiguration config = null, CancellationToken cancellationToken = default)
        {
            var plan = async
                ? AsyncPlans.GetValue(expression, e => NemoQueryParser.Parse(e, true))
                : SyncPlans.GetValue(expression, e => NemoQueryParser.Parse(e, false));
            var type = plan.ElementType;

            if (plan.IsCount)
            {
                var countArgs = async
                    ? new object[] { plan.Predicate, null, connection, config, cancellationToken }
                    : new object[] { plan.Predicate, null, connection, config };
                return GetOrAddClosedMethod(async ? CountAsyncMethod : CountMethod, type, plan.IsLongCount ? typeof(long) : typeof(int))
                    .Invoke(null, countArgs);
            }

            if (plan.Aggregate != null)
            {
                var aggregateArgs = async
                    ? new object[] { plan.Aggregate.Value, plan.AggregateProjection, plan.Predicate, null, connection, config, cancellationToken }
                    : new object[] { plan.Aggregate.Value, plan.AggregateProjection, plan.Predicate, null, connection, config };
                return GetOrAddClosedMethod(async ? AggregateAsyncMethod : AggregateMethod, type, plan.AggregateProperty.PropertyType)
                    .Invoke(null, aggregateArgs);
            }

            plan.GetPaging(out var page, out var pageSize, out var skipCount);

            var (sortingType, funcType) = SortingTypes.GetOrAdd(type, t => (typeof(Sorting<>).MakeGenericType(t), typeof(Func<,>).MakeGenericType(t, typeof(object))));
            var orderByArray = Array.CreateInstance(sortingType, plan.OrderBy.Count);
            for (var i = 0; i < plan.OrderBy.Count; i++)
            {
                var sort = plan.OrderBy[i];
                var sorting = (ISorting)System.Activator.CreateInstance(sortingType);
                var body = sort.KeySelector.Body;
                if (body.Type.IsValueType)
                {
                    body = Expression.Convert(body, typeof(object));
                }
                sorting.SetOrderBy(Expression.Lambda(funcType, body, sort.KeySelector.Parameters));
                sorting.Reverse = sort.Descending;
                orderByArray.SetValue(sorting, i);
            }

            return GetOrAddClosedMethod(async ? SelectAsyncMethod : SelectMethod, type)
                .Invoke(null, new object[] { plan.Predicate, null, connection, page, pageSize, skipCount, null, plan.SelectOption, config, orderByArray });
        }
    }
}
