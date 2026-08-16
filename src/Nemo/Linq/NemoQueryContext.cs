using System;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        // Executes the expression tree that is passed to it. 
        internal static object Execute(Expression expression, DbConnection connection = null, bool async = false, INemoConfiguration config = null)
        {
            var plan = NemoQueryParser.Parse(expression, async);
            var type = plan.ElementType;

            if (plan.IsCount)
            {
                return (async ? CountAsyncMethod : CountMethod).MakeGenericMethod(type, plan.IsLongCount ? typeof(long) : typeof(int))
                    .Invoke(null, new object[] { plan.Predicate, null, connection, config });
            }

            if (plan.Aggregate != null)
            {
                return (async ? AggregateAsyncMethod : AggregateMethod).MakeGenericMethod(type, plan.AggregateProperty.PropertyType)
                    .Invoke(null, new object[] { plan.Aggregate.Value, plan.AggregateProjection, plan.Predicate, null, connection, config });
            }

            plan.GetPaging(out var page, out var pageSize, out var skipCount);

            var funcType = typeof(Func<,>).MakeGenericType(type, typeof(object));
            var sortingType = typeof(Sorting<>).MakeGenericType(type);
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

            return (async ? SelectAsyncMethod : SelectMethod).MakeGenericMethod(type)
                .Invoke(null, new object[] { plan.Predicate, null, connection, page, pageSize, skipCount, null, plan.SelectOption, config, orderByArray });
        }
    }
}
