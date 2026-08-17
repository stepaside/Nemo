using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections;
using Nemo.Configuration;
using Nemo.Reflection;

namespace Nemo.Linq
{
    public class NemoQueryProvider : IAsyncQueryProvider, IQueryProvider
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
            if (result is IEagerLoadEnumerable && !typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                return ((IEnumerable)result).OfType<TResult>().FirstOrDefault();
            }
            return (TResult)result;
        }

        public object Execute(Expression expression)
        {
            return NemoQueryContext.Execute(expression, _connection, config: _config);
        }

        IAsyncQueryable<TElement> IAsyncQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new NemoQueryableAsync<TElement>(this, expression);
        }

        private static readonly MethodInfo MaterializeAsyncMethod = typeof(NemoQueryProvider).GetMethod(nameof(MaterializeAsync), BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly ConcurrentDictionary<Type, Func<NemoQueryProvider, Expression, IQueryable>> QueryableFactories = new ConcurrentDictionary<Type, Func<NemoQueryProvider, Expression, IQueryable>>();

        private static readonly ConcurrentDictionary<Type, Func<object, CancellationToken, Task<IEnumerable>>> Materializers = new ConcurrentDictionary<Type, Func<object, CancellationToken, Task<IEnumerable>>>();

        private static async Task<IEnumerable> MaterializeAsync<T>(object source, CancellationToken token)
            where T : class
        {
            return await ((IAsyncEnumerable<T>)source).ToEnumerableAsync(token).ConfigureAwait(false);
        }

        private static Func<object, CancellationToken, Task<IEnumerable>> GetMaterializer(Type elementType)
        {
            return Materializers.GetOrAdd(elementType, t => (Func<object, CancellationToken, Task<IEnumerable>>)MaterializeAsyncMethod.MakeGenericMethod(t).CreateDelegate(typeof(Func<object, CancellationToken, Task<IEnumerable>>)));
        }

        internal object ExecuteAsyncCore(Expression expression, CancellationToken token)
        {
            return NemoQueryContext.Execute(expression, _connection, true, _config, token);
        }

        public async ValueTask<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
        {
            var async = NemoQueryContext.Execute(expression, _connection, true, _config, token);
            if (typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                var type = Reflector.GetElementType(typeof(TResult));
                var items = await GetMaterializer(type)(async, token).ConfigureAwait(false);
                if (typeof(IList).IsAssignableFrom(typeof(TResult)))
                {
                    var list = List.Create(type);
                    foreach (var item in items)
                    {
                        list.Add(item);
                    }

                    if (typeof(TResult).IsArray)
                    {
                        return (TResult)(object)List.CreateArray(type, list);
                    }
                    return (TResult)list;
                }
                return (TResult)items;
            }
            else if (async is IEagerLoadEnumerableAsync && !typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                return await ((IAsyncEnumerable<TResult>)async).FirstOrDefaultAsync(token).ConfigureAwait(false);
            }
            else
            {
                return await ((Task<TResult>)async).ConfigureAwait(false);
            }
        }
    }
}