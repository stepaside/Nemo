using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Nemo.Collections;
using Nemo.Configuration;
using Nemo.Reflection;

namespace Nemo.Linq
{
    public class NemoQueryProvider : IQueryProvider
    {
        private readonly DbConnection _connection;
        private readonly INemoConfiguration _config;

        public NemoQueryProvider(DbConnection connection = null, INemoConfiguration config = null)
        {
            _connection = connection;
            _config = config;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new NemoQueryable<TElement>(this, expression);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = Reflector.GetElementType(expression.Type) ?? expression.Type;
            var factory = QueryableFactories.GetOrAdd(elementType, t =>
            {
                var queryableType = typeof(NemoQueryable<>).MakeGenericType(t);
                var constructor = queryableType.GetConstructor(new[] { typeof(NemoQueryProvider), typeof(Expression) });
                var providerParameter = Expression.Parameter(typeof(NemoQueryProvider), "provider");
                var expressionParameter = Expression.Parameter(typeof(Expression), "expression");
                return Expression.Lambda<Func<NemoQueryProvider, Expression, IQueryable>>(Expression.New(constructor, providerParameter, expressionParameter), providerParameter, expressionParameter).Compile();
            });
            return factory(this, expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            var result = NemoQueryContext.Execute(expression, _connection, config: _config);
            if (result is IEnumerable && !typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                return ((IEnumerable)result).OfType<TResult>().FirstOrDefault();
            }
            return (TResult)result;
        }

        public object Execute(Expression expression)
        {
            return NemoQueryContext.Execute(expression, _connection, config: _config);
        }

        private static readonly ConcurrentDictionary<Type, Func<NemoQueryProvider, Expression, IQueryable>> QueryableFactories = new ConcurrentDictionary<Type, Func<NemoQueryProvider, Expression, IQueryable>>();

        internal object ExecuteAsyncCore(Expression expression, CancellationToken token)
        {
            return NemoQueryContext.Execute(expression, _connection, true, _config, token);
        }
    }
}