using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Select Methods

        public static IEnumerable<T> Select<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }

            var provider = DialectFactory.GetProvider(connection, providerName);

            var parameters = new List<Param>();
            var sql = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sql }, new[] { typeof(T) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.SingleResult, connectionName, connection, types: t, cached: cached, config: config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Select<T, T1>(Expression<Func<T, T1, bool>> join, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0,
            bool? cached = null, SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin = SqlBuilder.GetSelectStatement(predicate, join, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin }, new[] { typeof(T), typeof(T1) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Select<T, T1, T2>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2 }, new[] { typeof(T), typeof(T1), typeof(T2) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        internal static IEnumerable<T> Select<T, T1, T2, T3>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Select<T, T1, T2, T3, T4>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin4 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, join4, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3, sqlJoin4 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) },
              (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Union<T>(params IEnumerable<T>[] sources)
            where T : class
        {
            IEnumerable<T> first = null;
            foreach (var source in sources)
            {
                if (first == null)
                {
                    first = source;
                }
                else if (first is EagerLoadEnumerable<T> eagerLoadEnumeable)
                {
                    first = eagerLoadEnumeable.Union(source);
                }
                else
                {
                    first = first.Union(source);
                }
            }
            return first ?? Enumerable.Empty<T>();
        }

        public static IEnumerable<TSource> Include<TSource, TInclude>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude, bool>> join)
            where TSource : class
            where TInclude : class
        {
            if (source is EagerLoadEnumerable<TSource> eagerSource)
            {
                return Union(source, Select(join, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IEnumerable<TSource> Include<TSource, TInclude1, TInclude2>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
        {
            if (source is EagerLoadEnumerable<TSource> eagerSource)
            {
                return Union(source, Select(join1, join2, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IEnumerable<TSource> Include<TSource, TInclude1, TInclude2, TInclude3>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2, Expression<Func<TInclude2, TInclude3, bool>> join3)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
            where TInclude3 : class
        {
            if (source is EagerLoadEnumerable<TSource> eagerSource)
            {
                return Union(source, Select(join1, join2, join3, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IEnumerable<TSource> Include<TSource, TInclude1, TInclude2, TInclude3, TInclude4>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2, Expression<Func<TInclude2, TInclude3, bool>> join3, Expression<Func<TInclude3, TInclude4, bool>> join4)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
            where TInclude3 : class
            where TInclude4 : class
        {
            if (source is EagerLoadEnumerable<TSource> eagerSource)
            {
                return Union(source, Select(join1, join2, join3, join4, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        #endregion

        #region Select Async Methods

        public static async Task<IEnumerable<T>> ToEnumerableAsync<T>(this IAsyncEnumerable<T> source, CancellationToken cancellationToken = default)
            where T : class
        {
            if (!(source is EagerLoadEnumerableAsync<T> loader)) return ToBlockingEnumerable(source, cancellationToken);

            var iterator = await loader.GetEnumeratorAsync(cancellationToken).ConfigureAwait(false);
            return iterator.AsEnumerable();
        }

        public static IAsyncEnumerable<T> SelectAsync<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }

            var provider = DialectFactory.GetProvider(connection, providerName);

            var parameters = new List<Param>();
            var sql = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);

            var result = new EagerLoadEnumerableAsync<T>(new[] { sql }, new[] { typeof(T) },
                (s, t, ct) => RetrieveImplemenationAsync<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.SingleResult, connectionName, connection, types: t, cached: cached, config: config, cancellationToken: ct), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IAsyncEnumerable<T> SelectAsync<T, T1>(Expression<Func<T, T1, bool>> join, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0,
            bool? cached = null, SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin = SqlBuilder.GetSelectStatement(predicate, join, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerableAsync<T>(new[] { sqlRoot, sqlJoin }, new[] { typeof(T), typeof(T1) },
                async (s, t, ct) => ((IMultiResult)await RetrieveImplemenationAsync<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config, cancellationToken: ct).ConfigureAwait(false)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IAsyncEnumerable<T> SelectAsync<T, T1, T2>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerableAsync<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2 }, new[] { typeof(T), typeof(T1), typeof(T2) },
                async (s, t, ct) => ((IMultiResult)await RetrieveImplemenationAsync<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config, cancellationToken: ct).ConfigureAwait(false)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        internal static IAsyncEnumerable<T> SelectAsync<T, T1, T2, T3>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerableAsync<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3) },
                async (s, t, ct) => ((IMultiResult)await RetrieveImplemenationAsync<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config, cancellationToken: ct).ConfigureAwait(false)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IAsyncEnumerable<T> SelectAsync<T, T1, T2, T3, T4>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, INemoConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                    connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var parameters = new List<Param>();
            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, parameters, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, parameters, orderBy);
            var sqlJoin4 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, join4, 0, 0, 0, false, provider, parameters, orderBy);

            var result = new EagerLoadEnumerableAsync<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3, sqlJoin4 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) },
                async (s, t, ct) => ((IMultiResult)await RetrieveImplemenationAsync<T>(s, OperationType.Sql, parameters.Count > 0 ? parameters : null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config, cancellationToken: ct).ConfigureAwait(false)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IAsyncEnumerable<T> UnionAsync<T>(params IAsyncEnumerable<T>[] sources)
            where T : class
        {
            IAsyncEnumerable<T> first = null;
            foreach (var source in sources)
            {
                if (first == null)
                {
                    first = source;
                }
                else if (first is EagerLoadEnumerableAsync<T> firstEager && source is EagerLoadEnumerableAsync<T> sourceEager)
                {
                    first = firstEager.Union(sourceEager);
                }
                else
                {
                    first = UnionAsyncCore(first, source);
                }
            }
            return first ?? EmptyAsync<T>();
        }

        private static async IAsyncEnumerable<T> UnionAsyncCore<T>(IAsyncEnumerable<T> first, IAsyncEnumerable<T> second, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var set = new HashSet<T>();
            await foreach (var item in first.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (set.Add(item)) yield return item;
            }
            await foreach (var item in second.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (set.Add(item)) yield return item;
            }
        }

        private static async IAsyncEnumerable<T> EmptyAsync<T>()
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        private static IEnumerable<T> ToBlockingEnumerable<T>(IAsyncEnumerable<T> source, CancellationToken cancellationToken)
        {
            var enumerator = source.GetAsyncEnumerator(cancellationToken);
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    yield return enumerator.Current;
                }
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        public static IAsyncEnumerable<TSource> IncludeAsync<TSource, TInclude>(this IAsyncEnumerable<TSource> source, Expression<Func<TSource, TInclude, bool>> join)
            where TSource : class
            where TInclude : class
        {
            if (source is EagerLoadEnumerableAsync<TSource> eagerSource)
            {
                return UnionAsync(source, SelectAsync(join, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IAsyncEnumerable<TSource> IncludeAsync<TSource, TInclude1, TInclude2>(this IAsyncEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
        {
            if (source is EagerLoadEnumerableAsync<TSource> eagerSource)
            {
                return UnionAsync(source, SelectAsync(join1, join2, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IAsyncEnumerable<TSource> IncludeAsync<TSource, TInclude1, TInclude2, TInclude3>(this IAsyncEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2, Expression<Func<TInclude2, TInclude3, bool>> join3)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
            where TInclude3 : class
        {
            if (source is EagerLoadEnumerableAsync<TSource> eagerSource)
            {
                return UnionAsync(source, SelectAsync(join1, join2, join3, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        public static IAsyncEnumerable<TSource> IncludeAsync<TSource, TInclude1, TInclude2, TInclude3, TInclude4>(this IAsyncEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2, Expression<Func<TInclude2, TInclude3, bool>> join3, Expression<Func<TInclude3, TInclude4, bool>> join4)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
            where TInclude3 : class
            where TInclude4 : class
        {
            if (source is EagerLoadEnumerableAsync<TSource> eagerSource)
            {
                return UnionAsync(source, SelectAsync(join1, join2, join3, join4, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption, connectionName: eagerSource.ConnectionName, connection: eagerSource.Connection, page: eagerSource.Page, pageSize: eagerSource.PageSize, skipCount: eagerSource.SkipCount, config: eagerSource.Configuration));
            }
            return source;
        }

        #endregion

    }
}
