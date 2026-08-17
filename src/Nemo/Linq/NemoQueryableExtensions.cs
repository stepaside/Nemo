using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Nemo.Linq
{
    /// <summary>
    /// Asynchronous execution and materialization methods for queries composed with <see cref="NemoQueryable{T}"/>.
    /// </summary>
    public static class NemoQueryableExtensions
    {
        private static readonly MethodInfo FirstMethod = GetMethod(nameof(FirstAsync), 2);
        private static readonly MethodInfo FirstWithPredicateMethod = GetMethod(nameof(FirstAsync), 3);
        private static readonly MethodInfo FirstOrDefaultMethod = GetMethod(nameof(FirstOrDefaultAsync), 2);
        private static readonly MethodInfo FirstOrDefaultWithPredicateMethod = GetMethod(nameof(FirstOrDefaultAsync), 3);
        private static readonly MethodInfo CountMethod = GetMethod(nameof(CountAsync), 2);
        private static readonly MethodInfo CountWithPredicateMethod = GetMethod(nameof(CountAsync), 3);
        private static readonly MethodInfo LongCountMethod = GetMethod(nameof(LongCountAsync), 2);
        private static readonly MethodInfo LongCountWithPredicateMethod = GetMethod(nameof(LongCountAsync), 3);
        private static readonly MethodInfo MaxMethod = GetMethod(nameof(MaxAsync), 3);
        private static readonly MethodInfo MinMethod = GetMethod(nameof(MinAsync), 3);
        private static readonly MethodInfo SumMethod = GetMethod(nameof(SumAsync), 3);
        private static readonly MethodInfo AverageMethod = GetMethod(nameof(AverageAsync), 3);

        private static MethodInfo GetMethod(string name, int parameterCount)
        {
            return typeof(NemoQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .First(m => m.Name == name && m.GetParameters().Length == parameterCount);
        }

        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var provider = GetProvider(source);
            var result = (IAsyncEnumerable<T>)provider.ExecuteAsyncCore(source.Expression, cancellationToken);
            await foreach (var item in result.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }

        public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            var list = new List<T>();
            await foreach (var item in source.AsAsyncEnumerable(cancellationToken).ConfigureAwait(false))
            {
                list.Add(item);
            }
            return list;
        }

        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            return (await source.ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();
        }

        public static Task<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            return FirstCoreAsync(source, FirstMethod, null, true, cancellationToken);
        }

        public static Task<T> FirstAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return FirstCoreAsync(source, FirstWithPredicateMethod, predicate, true, cancellationToken);
        }

        public static Task<T> FirstOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            return FirstCoreAsync(source, FirstOrDefaultMethod, null, false, cancellationToken);
        }

        public static Task<T> FirstOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return FirstCoreAsync(source, FirstOrDefaultWithPredicateMethod, predicate, false, cancellationToken);
        }

        public static Task<int> CountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            return (Task<int>)Execute(source, CountMethod, new[] { typeof(T) }, null, cancellationToken);
        }

        public static Task<int> CountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return (Task<int>)Execute(source, CountWithPredicateMethod, new[] { typeof(T) }, predicate, cancellationToken);
        }

        public static Task<long> LongCountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            return (Task<long>)Execute(source, LongCountMethod, new[] { typeof(T) }, null, cancellationToken);
        }

        public static Task<long> LongCountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return (Task<long>)Execute(source, LongCountWithPredicateMethod, new[] { typeof(T) }, predicate, cancellationToken);
        }

        public static Task<TResult> MaxAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
            where TResult : struct
        {
            return (Task<TResult>)Execute(source, MaxMethod, new[] { typeof(T), typeof(TResult) }, selector, cancellationToken);
        }

        public static Task<TResult> MinAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
            where TResult : struct
        {
            return (Task<TResult>)Execute(source, MinMethod, new[] { typeof(T), typeof(TResult) }, selector, cancellationToken);
        }

        public static Task<TResult> SumAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
            where TResult : struct
        {
            return (Task<TResult>)Execute(source, SumMethod, new[] { typeof(T), typeof(TResult) }, selector, cancellationToken);
        }

        public static Task<TResult> AverageAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
            where TResult : struct
        {
            return (Task<TResult>)Execute(source, AverageMethod, new[] { typeof(T), typeof(TResult) }, selector, cancellationToken);
        }

        private static async Task<T> FirstCoreAsync<T>(IQueryable<T> source, MethodInfo method, Expression<Func<T, bool>> predicate, bool throwOnEmpty, CancellationToken cancellationToken)
        {
            var result = (IAsyncEnumerable<T>)Execute(source, method, new[] { typeof(T) }, predicate, cancellationToken);
            await foreach (var item in result.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                return item;
            }
            if (throwOnEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements.");
            }
            return default;
        }

        private static object Execute(IQueryable source, MethodInfo method, Type[] typeArguments, LambdaExpression lambda, CancellationToken cancellationToken)
        {
            var provider = GetProvider(source);
            var arguments = lambda != null
                ? new[] { source.Expression, Expression.Quote(lambda), Expression.Constant(cancellationToken) }
                : new[] { source.Expression, Expression.Constant(cancellationToken) };
            var expression = Expression.Call(method.MakeGenericMethod(typeArguments), arguments);
            return provider.ExecuteAsyncCore(expression, cancellationToken);
        }

        private static NemoQueryProvider GetProvider(IQueryable source)
        {
            if (source.Provider is NemoQueryProvider provider)
            {
                return provider;
            }
            throw new InvalidOperationException("The source IQueryable is not backed by the Nemo LINQ provider.");
        }
    }
}
