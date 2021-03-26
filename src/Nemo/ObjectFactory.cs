using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Fn.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Transactions;
using ObjectActivator = Nemo.Reflection.Activator.ObjectActivator;

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Declarations

        public const string OperationRetrieve = "Retrieve";
        public const string OperationInsert = "Insert";
        public const string OperationUpdate = "Update";
        public const string OperationDelete = "Delete";
        public const string OperationDestroy = "Destroy";

        #endregion

        #region Instantiation Methods

        public static T Create<T>()
            where T : class
        {
            return Create<T>(typeof(T).IsInterface);
        }
        
        public static T Create<T>(bool isInterface)
            where T : class
        {
            var value = isInterface ? Adapter.Implement<T>() : FastActivator<T>.New();

            TrySetObjectState(value);

            return value;
        }

        public static object Create(Type targetType)
        {
            if (targetType == null) return null;

            var value = targetType.IsInterface ? Adapter.InternalImplement(targetType)() : Reflection.Activator.CreateDelegate(targetType)();

            TrySetObjectState(value);

            return value;
        }

        #endregion

        #region Map Methods

        public static IDictionary<string, object> ToDictionary(this object source)
        {
            var mapper = Mapper.CreateDelegate(source.GetType());
            var map = new Dictionary<string, object>();
            mapper(source, map);
            return map;
        }

        public static object Map(object source, Type targetType)
        {
            if (source == null) return null;
            var target = Create(targetType);
            var indexer = MappingFactory.IsIndexer(source);
            if (indexer)
            {
                var autoTypeCoercion = (ConfigurationFactory.Get(targetType)?.AutoTypeCoercion).GetValueOrDefault();
                Mapper.CreateDelegate(MappingFactory.GetIndexerType(source), targetType, indexer, autoTypeCoercion)(source, target);
            }
            else
            {
                Mapper.CreateDelegate(source.GetType(), targetType, indexer, false)(source, target);
            }
            return target;
        }

        internal static object Map(object source, Type targetType, bool autoTypeCoercion)
        {
            if (source == null) return null;
            var target = Create(targetType);
            var indexer = MappingFactory.IsIndexer(source);
            Mapper.CreateDelegate(indexer ? MappingFactory.GetIndexerType(source) : source.GetType(), targetType, indexer, autoTypeCoercion)(source, target);
            return target;
        }

        public static T Map<T>(object source)
            where T : class
        {
            return (T)Map(source, typeof(T));
        }

        internal static T Map<T>(object source, bool autoTypeCoercion)
            where T : class
        {
            return (T)Map(source, typeof(T), autoTypeCoercion);
        }

        public static TResult Map<TSource, TResult>(TSource source)
            where TResult : class
            where TSource : class
        {
            var target = Create<TResult>(typeof(TResult).IsInterface);
            return Map(source, target);
        }

        internal static TResult Map<TSource, TResult>(TSource source, bool autoTypeCoercion)
            where TResult : class
            where TSource : class
        {
            var target = Create<TResult>(typeof(TResult).IsInterface);
            return Map(source, target, autoTypeCoercion);
        }

        public static TResult Map<TSource, TResult>(TSource source, TResult target)
            where TResult : class
            where TSource : class
        {
            var indexer = MappingFactory.IsIndexer(source);

            if (indexer)
            {
                var autoTypeCoercion = (ConfigurationFactory.Get<TResult>()?.AutoTypeCoercion).GetValueOrDefault();
                if (autoTypeCoercion)
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapperWithTypeCoercion<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapperWithTypeCoercion<TSource, TResult>.Map(source, target);
                    }
                }
                else
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapper<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                FastMapper<TSource, TResult>.Map(source, target);
            }
            return target;
        }

        internal static TResult Map<TSource, TResult>(TSource source, TResult target, bool autoTypeCoercion)
           where TResult : class
           where TSource : class
        {
            var indexer = MappingFactory.IsIndexer(source);

            if (indexer)
            {
                if (autoTypeCoercion)
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapperWithTypeCoercion<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapperWithTypeCoercion<TSource, TResult>.Map(source, target);
                    }
                }
                else
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapper<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                FastMapper<TSource, TResult>.Map(source, target);
            }
            return target;
        }

        public static T Map<T>(IDictionary<string, object> source)
            where T : class
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(IDictionary<string, object> source, bool autoTypeCoercion)
            where T : class
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(IDictionary<string, object> source, T target)
            where T : class
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(IDictionary<string, object> source, T target, bool autoTypeCoercion)
            where T : class
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<IDictionary<string, object>, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
               FastIndexerMapper<IDictionary<string, object>, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        public static T Map<T>(DataRow source)
            where T : class
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(DataRow source, bool autoTypeCoercion)
            where T : class
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(DataRow source, T target)
            where T : class
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(DataRow source, T target, bool autoTypeCoercion)
            where T : class
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<DataRow, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
                FastIndexerMapper<DataRow, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        public static T Map<T>(IDataReader source)
            where T : class
        {
            return Map<T>((IDataRecord)source);
        }

        internal static T Map<T>(IDataReader source, bool autoTypeCoercion)
           where T : class
        {
            return Map<T>((IDataRecord)source, autoTypeCoercion);
        }

        public static void Map<T>(IDataReader source, T target)
            where T : class
        {
            Map((IDataRecord)source, target);
        }

        internal static void Map<T>(IDataReader source, T target, bool autoTypeCoercion)
           where T : class
        {
            Map((IDataRecord)source, target, autoTypeCoercion);
        }

        public static T Map<T>(IDataRecord source)
            where T : class
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(IDataRecord source, bool autoTypeCoercion)
            where T : class
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(IDataRecord source, T target)
            where T : class
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(IDataRecord source, T target, bool autoTypeCoercion)
            where T : class
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<IDataRecord, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
                FastIndexerMapper<IDataRecord, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        #endregion

        #region Bind Methods

        /// <summary>
        /// Binds interface implementation to the existing object type.
        /// </summary>
        public static T Bind<T>(object source)
            where T : class
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            var target = Adapter.Bind<T>(source);
            return target;
        }

        #endregion

        #region Wrap Methods

        /// <summary>
        /// Converts a dictionary to an instance of the object specified by the interface.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="map"></param>
        /// <param name="simpleAndComplexProperties"></param>
        /// <returns></returns>
        public static T Wrap<T>(IDictionary<string, object> map, bool simpleAndComplexProperties = false)
             where T : class
        {
            var wrapper = simpleAndComplexProperties ? FastComplexWrapper<T>.Instance : FastWrapper<T>.Instance;
            return (T)wrapper(map);
        }

        public static object Wrap(IDictionary<string, object> value, Type targetType, bool simpleAndComplexProperties = false)
        {
            return Adapter.Wrap(value, targetType, simpleAndComplexProperties);
        }

        #endregion

        #region Aggregate Methods

        public static int Count<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
            where T : class
        {
            return Count<T, int>(predicate, connectionName, connection);
        }

        public static long LongCount<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
        {
            return Count<T, long>(predicate, connectionName, connection);
        }

        internal static TResult Count<T, TResult>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
            where T : class
            where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }
            var sql = SqlBuilder.GetSelectCountStatement(predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<TResult>(sql, connection: connection);
        }

        public static TResult Max<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.MAX, projection, predicate, connectionName, connection);
        }

        public static TResult Min<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.MIN, projection, predicate, connectionName, connection);
        }

        public static TResult Sum<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.SUM, projection, predicate, connectionName, connection);
        }

        public static TResult Average<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.AVG, projection, predicate, connectionName, connection);
        }

        internal enum AggregateNames { MAX, MIN, SUM, AVG }

        internal static TResult Aggregate<T, TResult>(AggregateNames aggregateName, Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
           where T : class
           where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }
            var sql = SqlBuilder.GetSelectAggregationStatement(aggregateName.ToString(), projection, predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<TResult>(sql, connection: connection);
        }

        #endregion

        #region Select Methods

        public static IEnumerable<T> Select<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, IConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }

            var provider = DialectFactory.GetProvider(connection, providerName);

            var sql = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sql }, new[] { typeof(T) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.SingleResult, connectionName, connection, types: t, cached: cached, config: config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Select<T, T1>(Expression<Func<T, T1, bool>> join, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0,
            bool? cached = null, SelectOption selectOption = SelectOption.All, IConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                    connection = DbFactory.CreateConnection(connectionName, typeof(T));
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin = SqlBuilder.GetSelectStatement(predicate, join, 0, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin }, new[] { typeof(T), typeof(T1) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }
        
        private static IEnumerable<T> Select<T, T1, T2>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null, 
            SelectOption selectOption = SelectOption.All, IConfiguration config = null, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            if (provider == null)
            {
                string providerName = null;
                if (connection == null)
                {
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                    connection = DbFactory.CreateConnection(connectionName, typeof(T));
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2 }, new[] { typeof(T), typeof(T1), typeof(T2) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        internal static IEnumerable<T> Select<T, T1, T2, T3>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, IConfiguration config = null, params Sorting<T>[] orderBy)
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
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                    connection = DbFactory.CreateConnection(connectionName, typeof(T));
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3) },
                (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

            return result;
        }

        private static IEnumerable<T> Select<T, T1, T2, T3, T4>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, int skipCount = 0, bool? cached = null, 
            SelectOption selectOption = SelectOption.All, IConfiguration config = null, params Sorting<T>[] orderBy)
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
                    providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                    connection = DbFactory.CreateConnection(connectionName, typeof(T));
                }

                provider = DialectFactory.GetProvider(connection, providerName);
            }

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, skipCount, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, 0, false, provider, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, 0, false, provider, orderBy);
            var sqlJoin4 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, join4, 0, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3, sqlJoin4 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) },
              (s, t) => ((IMultiResult)RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached, config: config)).Aggregate<T>(config), predicate, provider, selectOption, connectionName, connection, page, pageSize, skipCount, config);

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
                else if (first is EagerLoadEnumerable<T>)
                {
                    first = ((EagerLoadEnumerable<T>)first).Union(source);
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

        #region Retrieve Methods

        public static T RetrieveScalar<T>(string sql, Param[] parameters = null, string connectionName = null, DbConnection connection = null, string schema = null)
            where T : struct
        {
            var response = connection != null
                ? Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connection: connection, schema: schema)
                : Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connectionName: connectionName, schema: schema);

            var value = response.Value;
            if (value == null)
            {
                return default(T);
            }
				
			return (T)Reflector.ChangeType(value, typeof(T));
        }

        private static IEnumerable<TResult> RetrieveImplemenation<TResult>(string operation, OperationType operationType, IList<Param> parameters, OperationReturnType returnType, string connectionName, DbConnection connection, Func<object[], TResult> map = null, IList<Type> types = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where TResult : class
        {
            Log.CaptureBegin(() => $"RetrieveImplemenation: {typeof(TResult).FullName}::{operation}");
            IEnumerable<TResult> result;
            
            string queryKey = null;
            IdentityMap<TResult> identityMap = null;
            
            if (!cached.HasValue)
            {
                if (config == null)
                {
                    config = ConfigurationFactory.Get<TResult>();
                }

                cached = config.DefaultCacheRepresentation != CacheRepresentation.None;
            }

            if (cached.Value)
            {
                if (config == null)
                {
                    config = ConfigurationFactory.Get<TResult>();
                }

                queryKey = GetQueryKey<TResult>(operation, parameters ?? new Param[] { }, returnType);

                Log.CaptureBegin(() => $"Retrieving from L1 cache: {queryKey}");

                if (returnType == OperationReturnType.MultiResult)
                {
                    result = config.ExecutionContext.Get(queryKey) as IEnumerable<TResult>;
                }
                else
                {
                    identityMap = Identity.Get<TResult>(config);
                    result = identityMap.GetIndex(queryKey);
                }
                
                Log.CaptureEnd();

                if (result != null)
                {
                    Log.Capture(() => $"Found in L1 cache: {queryKey}");
                    
                    if (returnType == OperationReturnType.MultiResult)
                    {
                        ((IMultiResult)result).Reset();
                    }
                    
                    Log.CaptureEnd();
                    return result;
                }
                Log.Capture(() => $"Not found in L1 cache: {queryKey}");
            }

            result = RetrieveItems(operation, parameters, operationType, returnType, connectionName, connection, types, map, cached.Value, schema, config, identityMap);
            
            if (queryKey != null)
            {
                Log.CaptureBegin(() => $"Saving to L1 cache: {queryKey}");

                if (!(result is IList<TResult>) && !(result is IMultiResult))
                {
                    if (config.DefaultCacheRepresentation == CacheRepresentation.List)
                    {
                        result = result.ToList();
                    }
                    else
                    {
                        result = result.AsStream() ?? Enumerable.Empty<TResult>();
                    }
                }

                if (identityMap != null)
                {
                    result = identityMap.AddIndex(queryKey, result);
                }
                else if (result is IMultiResult)
                {
                    config.ExecutionContext.Set(queryKey, result);
                }

                Log.CaptureEnd();
            }

            Log.CaptureEnd();
            return result;
        }

        private static IEnumerable<T> RetrieveItems<T>(string operation, IList<Param> parameters, OperationType operationType, OperationReturnType returnType, string connectionName, DbConnection connection, IList<Type> types, Func<object[], T> map, bool cached, string schema, IConfiguration config, IdentityMap<T> identityMap)
            where T : class
        {
            if (operationType == OperationType.Guess)
            {
                operationType = operation.Any(char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            var operationText = GetOperationText(typeof(T), operation, operationType, schema, config);

            var response = connection != null 
                ? Execute(operationText, parameters, returnType, connection: connection, operationType: operationType, types: types, schema: schema) 
                : Execute(operationText, parameters, returnType, connectionName: connectionName, operationType: operationType, types: types, schema: schema);

            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }
            var result = Translate(response, map, types, config, identityMap);
            return result;
        }

        private static string GetQueryKey<T>(string operation, IEnumerable<Param> parameters, OperationReturnType returnType)
        {
            var hash = Hash.Compute(Encoding.UTF8.GetBytes(returnType + "/" + operation + "/" + string.Join(",", parameters.OrderBy(p => p.Name).Select(p => p.Name + "=" + p.Value))));
            return typeof(T).FullName + "/" + hash;
        }
        
        /// <summary>
        /// Retrieves an enumerable of type T using provided rule parameters.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<TResult> Retrieve<TResult, T1, T2, T3, T4>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, T4, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
            where TResult : class
        {
            var fakeType = typeof(Fake);
            var realTypes = new List<Type> { typeof(TResult) };
            if (fakeType != typeof(T1))
            {
                realTypes.Add(typeof(T1));
            }
            if (fakeType != typeof(T2))
            {
                realTypes.Add(typeof(T2));
            }
            if (fakeType != typeof(T3))
            {
                realTypes.Add(typeof(T3));
            }
            if (fakeType != typeof(T4))
            {
                realTypes.Add(typeof(T4));
            }

            if (config == null)
            {
                config = ConfigurationFactory.Get<TResult>();
            }
                        
            var returnType = OperationReturnType.SingleResult;

            Func<object[], TResult> func = null;
            if (map == null && realTypes.Count > 1)
            {
                returnType = OperationReturnType.MultiResult;
            }
            else if (map != null && realTypes.Count > 1)
            {
                switch (realTypes.Count)
                {
                    case 5:
                        func = args => map((TResult)args[0], (T1)args[1], (T2)args[2], (T3)args[3], (T4)args[4]);
                        break;
                    case 4:
                        func = args => map.Curry((TResult)args[0], (T1)args[1], (T2)args[2], (T3)args[3])(null);
                        break;
                    case 3:
                        func = args => map.Curry((TResult)args[0], (T1)args[1], (T2)args[2])(null, null);
                        break;
                    case 2:
                        func = args => map.Curry((TResult)args[0], (T1)args[1])(null, null, null);
                        break;
                }
            }

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            var parameterList = ExtractParameters<TResult>(operation, parameters);
            return RetrieveImplemenation(command, commandType, parameterList, returnType, connectionName, connection, func, realTypes, schema, cached, config);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1, T2, T3>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where T2 : class
            where T3 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, t3, f4) => map(t, t1, t2, t3) : (Func<TResult, T1, T2, T3, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1, T2>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where T2 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, f3, f4) => map(t, t1, t2) : (Func<TResult, T1, T2, Fake, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, f1, f2, f3) => map(t, t1) : (Func<TResult, T1, Fake, Fake, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config);
        }

        public static IEnumerable<T> Retrieve<T>(string operation = OperationRetrieve, string sql = null, object parameters = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T : class
        {
            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            var parameterList = ExtractParameters<T>(operation, parameters);
            return RetrieveImplemenation<T>(command, commandType, parameterList, OperationReturnType.SingleResult, connectionName, connection, null, new[] { typeof(T) }, schema, cached, config);
        }

        private static IList<Param> ExtractParameters<T>(string operation, object parameters) 
            where T : class
        {
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                switch (parameters)
                {
                    case ParamList list:
                        parameterList = list.GetParameters(typeof(T), operation);
                        break;
                    case Param[] array:
                        parameterList = array;
                        break;
                    case IDictionary<string, object> map:
                        parameterList = map.Select(p => new Param { Name = p.Key, Value = p.Value }).ToArray();
                        break;
                    default:
                        if (parameters is IList items)
                        {
                            var dbParameters = items.OfType<IDataParameter>().ToArray();
                            if (dbParameters.Length == items.Count)
                            {
                                parameterList = dbParameters.Select(p => new Param(p)).ToArray();
                            }
                        }
                        else if (Reflector.IsAnonymousType(parameters.GetType()))
                        {
                            parameterList = parameters.ToDictionary().Select(p => new Param { Name = p.Key, Value = p.Value }).ToArray();
                        }
                        break;
                }
            }

            return parameterList;
        }

        internal class Fake { }

        #endregion

        #region Insert/Update/Delete/Execute Methods
        
        private static IEnumerable<OperationRequest> BuildBatchInsert<T>(IEnumerable<T> items, DbTransaction transaction, bool captureException, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, DialectProvider provider, int batchSize = 500)
             where T : class
        {
            var statementId = 0;
            var insertSql = new StringBuilder();
            var insertParameters = new List<Param>();

            var batches = items.Split(batchSize <= 0 ? 500 : batchSize);
            foreach (var batch in batches)
            {
                var entities = batch as T[] ?? batch.ToArray();
                entities.GenerateKeys();

                foreach (var item in entities)
                {
                    var parameters = ObjectExtensions.GetInsertParameters(item, propertyMap, statementId++);
                    var sql = SqlBuilder.GetInsertStatement(typeof(T), parameters, provider);
                    insertSql.Append(sql).AppendLine(";");
                    insertParameters.AddRange(parameters);
                }

                var request = new OperationRequest
                {
                    Parameters = insertParameters,
                    ReturnType = OperationReturnType.NonQuery,
                    Transaction = transaction,
                    CaptureException = captureException,
                    OperationType = OperationType.Sql,
                    Operation = insertSql.ToString()
                };

                yield return request;
            }
        }

        public static long Insert<T>(IEnumerable<T> items, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, IConfiguration config = null)
            where T : class
        {
            var count = 0L;
            var connectionOpenedHere = false;
            var externalTransaction = transaction != null;
            var externalConnection = externalTransaction || connection != null;

            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            if (externalTransaction)
            {
                connection = transaction.Connection;
            }
            if (!externalConnection)
            {
                connection = DbFactory.CreateConnection(connectionName ?? config.DefaultConnectionName);
            }

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                    connectionOpenedHere = true;
                }
                if (transaction == null)
                {
                    transaction = connection.BeginTransaction();
                }

                var propertyMap = Reflector.GetPropertyMap<T>();
                var provider = DialectFactory.GetProvider(transaction.Connection);

                var requests = BuildBatchInsert(items, transaction, captureException, propertyMap, provider);
                count = requests.Select(Execute<T>).Where(response => !response.HasErrors).Aggregate(count, (current, response) => current + response.RecordsAffected);
                transaction.Commit();

                return count;
            }
            catch (Exception ex)
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                if (connectionOpenedHere)
                {
                    connection.Clone();
                }

                if (!externalConnection)
                {
                    connection.Dispose();
                }
            }
        }

        public static OperationResponse Insert<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Insert<T>(parameters.GetParameters(typeof(T), OperationInsert), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Insert<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            
            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            if (config.GenerateInsertSql)
            {
                request.Operation = SqlBuilder.GetInsertStatement(typeof(T), parameters, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationInsert;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Update<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Update<T>(parameters.GetParameters(typeof(T), OperationUpdate), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Update<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            
            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }
            
            if (config.GenerateUpdateSql)
            {
                var partition = parameters.Partition(p => p.IsPrimaryKey);
                // if p.IsPrimaryKey is not set then
                // we need to infer it from reflected property 
                if (partition.Item1.Count == 0)
                {
                    var propertyMap = Reflector.GetPropertyMap<T>();
                    var pimaryKeySet = propertyMap.Values.Where(p => p.IsPrimaryKey).ToDictionary(p => p.ParameterName ?? p.PropertyName, p => p.MappedColumnName);
                    partition = parameters.Partition(p =>
                    {
                        if (!pimaryKeySet.TryGetValue(p.Name, out var column)) return false;
                        p.Source = column;
                        p.IsPrimaryKey = true;
                        return true;
                    });
                }

                request.Operation = SqlBuilder.GetUpdateStatement(typeof(T), partition.Item2, partition.Item1, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationUpdate;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Delete<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Delete<T>(parameters.GetParameters(typeof(T), OperationDelete), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Delete<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };

            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            if (config.GenerateDeleteSql)
            {
                string softDeleteColumn = null; 
                var map = MappingFactory.GetEntityMap<T>();
                if (map != null)
                {
                    softDeleteColumn = map.SoftDeleteColumnName;
                }

                if (softDeleteColumn == null)
                {
                    var attr = Reflector.GetAttribute<T, TableAttribute>();
                    if (attr != null)
                    {
                        softDeleteColumn = attr.SoftDeleteColumn;
                    }
                }

                var partition = parameters.Partition(p => p.IsPrimaryKey);
                // if p.IsPrimaryKey is not set then
                // we need to infer it from reflected property 
                if (partition.Item1.Count == 0)
                {
                    var propertyMap = Reflector.GetPropertyMap<T>();
                    var pimaryKeySet = propertyMap.Values.Where(p => p.IsPrimaryKey).ToDictionary(p => p.ParameterName ?? p.PropertyName, p => p.MappedColumnName);
                    partition = parameters.Partition(p =>
                    {
                        if (!pimaryKeySet.TryGetValue(p.Name, out var column)) return false;
                        p.Source = column;
                        p.IsPrimaryKey = true;
                        return true;
                    });
                }
                
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), partition.Item1, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName), softDeleteColumn);
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationDelete;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Destroy<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Destroy<T>(parameters.GetParameters(typeof(T), OperationDestroy), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Destroy<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };

            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            if (config.GenerateDeleteSql)
            {
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), parameters, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationDestroy;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        internal static OperationResponse Execute(string operationText, IList<Param> parameters, OperationReturnType returnType, OperationType operationType, IList<Type> types = null, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, string schema = null)
        {
            var rootType = types?[0];

            DbConnection dbConnection ;
            var closeConnection = false;

            if (transaction != null)
            {
                dbConnection = transaction.Connection;
            }
            else if (connection != null)
            {
                dbConnection = connection;
            }
            else
            {
                dbConnection = DbFactory.CreateConnection(connectionName, rootType);
                closeConnection = true;
            }
            
            if (returnType == OperationReturnType.Guess)
            {
                if (operationText.IndexOf("insert", StringComparison.OrdinalIgnoreCase) > -1
                         || operationText.IndexOf("update", StringComparison.OrdinalIgnoreCase) > -1
                         || operationText.IndexOf("delete", StringComparison.OrdinalIgnoreCase) > -1)
                {
                    returnType = OperationReturnType.NonQuery;
                }
                else
                {
                    returnType = OperationReturnType.SingleResult;
                }
            }

            Dictionary<DbParameter, Param> outputParameters = null;

            var command = dbConnection.CreateCommand();
            command.CommandText = operationText;
            command.CommandType = operationType == OperationType.StoredProcedure ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandTimeout = 0;
            if (parameters != null)
            {
                for (var i = 0; i < parameters.Count; ++i)
                {
                    var parameter = parameters[i];
                    var dbParam = command.CreateParameter();
                    dbParam.ParameterName = parameter.Name.TrimStart('@', '?', ':');
                    dbParam.Direction = parameter.Direction;
                    dbParam.Value = parameter.Value ?? DBNull.Value;
                    
                    if (parameter.Value != null)
                    {
                        if (parameter.Size > -1)
                        {
                            dbParam.Size = parameter.Size;
                        }

                        dbParam.DbType = parameter.DbType ?? Reflector.ClrToDbType(parameter.Type);
                    }
                    else if (parameter.DbType != null)
                    {
                        dbParam.DbType = parameter.DbType.Value;
                    }

                    if (dbParam.Direction == ParameterDirection.Output)
                    {
                        if (outputParameters == null)
                        {
                            outputParameters = new Dictionary<DbParameter, Param>();
                        }
                        outputParameters.Add(dbParam, parameter);
                    }

                    command.Parameters.Add(dbParam);
                }
            }

            if (dbConnection.State != ConnectionState.Open)
            {
                dbConnection.Open();
            }

            var response = new OperationResponse { ReturnType = returnType };
            try
            {
                switch (returnType)
                {
                    case OperationReturnType.NonQuery:
                        response.RecordsAffected = command.ExecuteNonQuery();
                        break;
                    case OperationReturnType.MultiResult:
                    case OperationReturnType.SingleResult:
                    case OperationReturnType.SingleRow:
                        var behavior = CommandBehavior.Default;
                        switch (returnType)
                        {
                            case OperationReturnType.SingleResult:
                                behavior = CommandBehavior.SingleResult;
                                break;
                            case OperationReturnType.SingleRow:
                                behavior = CommandBehavior.SingleRow;
                                break;
                        }

                        if (closeConnection)
                        {
                            behavior |= CommandBehavior.CloseConnection;
                        }

                        closeConnection = false;
                        response.Value = command.ExecuteReader(behavior);
                        break;
                    case OperationReturnType.Scalar:
                        response.Value = command.ExecuteScalar();
                        break;
                }

                // Handle output parameters
                if (outputParameters != null)
                {
                    foreach (var entry in outputParameters)
                    {
                        entry.Value.Value = Convert.IsDBNull(entry.Key.Value) ? null : entry.Key.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                if (captureException)
                {
                    response.Exception = ex;
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                command.Dispose();
                if (dbConnection != null && (closeConnection || response.HasErrors))
                {
                    dbConnection.Close();
                }
            }

            return response;
        }

        public static OperationResponse Execute<T>(OperationRequest request)
            where T : class
        {
            if (request.Types == null)
            {
                request.Types = new[] { typeof(T) };
            }

            var operationType = request.OperationType;
            if (operationType == OperationType.Guess)
            {
                operationType = request.Operation.Any(char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            var operationText = GetOperationText(typeof(T), request.Operation, request.OperationType, request.SchemaName, ConfigurationFactory.Get<T>());

            var response = request.Connection != null 
                ? Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName) 
                : Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName);
            return response;
        }

        #endregion

        #region Translate Methods

        public static IEnumerable<T> Translate<T>(OperationResponse response, IConfiguration config)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();
            return Translate<T>(response, null, null, config, Identity.Get<T>(config));
        }

        private static IEnumerable<T> Translate<T>(OperationResponse response, Func<object[], T> map, IList<Type> types, IConfiguration config, IIdentityMap identityMap)
            where T : class
        {
            var cached = config.DefaultCacheRepresentation != CacheRepresentation.None;
            var mode = config.DefaultMaterializationMode;

           var value = response?.Value;
            if (value == null)
            {
                return Enumerable.Empty<T>();
            }

            var isInterface = Reflector.GetReflectedType<T>().IsInterface;

            switch (value)
            {
                case IDataReader reader:
                    if (map != null || types == null || types.Count == 1) return ConvertDataReader(reader, map, types, isInterface, config);
                    var multiResultItems = ConvertDataReaderMultiResult(reader, types, isInterface, config);
                    return (IEnumerable<T>)MultiResult.Create(types, multiResultItems, cached, config);
                case DataSet dataSet:
                    return ConvertDataSet<T>(dataSet, isInterface, config, identityMap);
                case DataTable dataTable:
                    return ConvertDataTable<T>(dataTable, isInterface, config, identityMap);
                case DataRow dataRow:
                    return ConvertDataRow<T>(dataRow, isInterface, config, identityMap, null).Return();
                case T item:
                    return item.Return();
                case IList<T> genericList:
                    return genericList;
                case IList list:
                    return list.Cast<object>().Select(i => mode == MaterializationMode.Exact ? Map<T>(i, config.AutoTypeCoercion) : Bind<T>(i));
            }

            return Bind<T>(value).Return();
        }

        private static IEnumerable<T> ConvertDataSet<T>(DataSet dataSet, bool isInterface, IConfiguration config, IIdentityMap identityMap)
             where T : class
        {
            var tableName = GetTableName(typeof(T));
            return dataSet.Tables.Count != 0 ? ConvertDataTable<T>(dataSet.Tables.Contains(tableName) ? dataSet.Tables[tableName] : dataSet.Tables[0], isInterface, config, identityMap) : Enumerable.Empty<T>();
        }

        private static IEnumerable<T> ConvertDataTable<T>(DataTable table, bool isInterface, IConfiguration config, IIdentityMap identityMap)
             where T : class
        {
            return ConvertDataTable<T>(table.Rows.Cast<DataRow>(), isInterface, config, identityMap);
        }

        private static IEnumerable<T> ConvertDataTable<T>(IEnumerable<DataRow> table, bool isInterface, IConfiguration config, IIdentityMap identityMap)
             where T : class
        {
            var primaryKey = GetPrimaryKeyColumns(typeof(T));
            return table.Select(row => ConvertDataRow<T>(row, isInterface, config, identityMap, primaryKey));
        }

        private static IEnumerable<object> ConvertDataTable(IEnumerable<DataRow> table, Type targetType, bool isInterface, IConfiguration config, IIdentityMap identityMap)
        {
            var primaryKey = GetPrimaryKeyColumns(targetType);
            return table.Select(row => ConvertDataRow(row, targetType, isInterface, config, identityMap, primaryKey));
        }
        
        private static T ConvertDataRow<T>(DataRow row, bool isInterface, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
             where T : class
        {
            var result = identityMap.GetEntityByKey<DataRow, T>(row.GetKeySelector(primaryKey), out var hash);

            if (result != null) return result;

            var value = config.DefaultMaterializationMode == MaterializationMode.Exact || !isInterface ? Map<T>(row, config.AutoTypeCoercion) : Wrap<T>(GetSerializableDataRow(row));
            TrySetObjectState(value);

            // Write-through for identity map
            identityMap.WriteThrough(value, hash);

            LoadRelatedData(row, value, typeof(T), config, identityMap, primaryKey);
            
            return value;
        }

        private static object ConvertDataRow(DataRow row, Type targetType, bool isInterface, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
        {
            string hash = null;

            if (identityMap != null)
            {
                var primaryKeyValue = new SortedDictionary<string, object>(primaryKey.ToDictionary(k => k, k => row[k]), StringComparer.Ordinal);
                hash = primaryKeyValue.ComputeHash(targetType);

                if (identityMap.TryGetValue(hash, out var result))
                {
                    return result;
                }
            }

            var value = config.DefaultMaterializationMode == MaterializationMode.Exact || !isInterface ? Map((object)row, targetType, config.AutoTypeCoercion) : Wrap(GetSerializableDataRow(row), targetType);
            TrySetObjectState(value);

            // Write-through for identity map
            if (identityMap != null && value != null && hash != null)
            {
                identityMap.Set(hash, value);
            }
            
            LoadRelatedData(row, value, targetType, config, identityMap, primaryKey);

            return value;
        }

        private static void LoadRelatedData(DataRow row, object value, Type targetType, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
        {
            var table = row.Table;
            if (table.ChildRelations.Count <= 0) return;

            var propertyMap = Reflector.GetPropertyMap(targetType);
            var relations = table.ChildRelations.Cast<DataRelation>().ToArray();

            primaryKey = primaryKey ?? GetPrimaryKeyColumns(targetType);
            
            foreach (var p in propertyMap)
            {
                // By convention each relation should end with the name of the property prefixed with underscore
                var relation = relations.FirstOrDefault(r => r.RelationName.EndsWith("_" + p.Key.Name));

                if (relation == null) continue;
                    
                var childRows = row.GetChildRows(relation);
                    
                if (childRows.Length <= 0) continue;

                object propertyValue = null;
                if (p.Value.IsDataEntity)
                {
                    var propertyKey = GetPrimaryKeyColumns(p.Key.PropertyType);
                    IIdentityMap relatedIdentityMap = null;
                    if (identityMap != null)
                    {
                        relatedIdentityMap = Identity.Get(p.Key.PropertyType, config);
                    }
                    propertyValue = ConvertDataRow(childRows[0], p.Key.PropertyType, p.Key.PropertyType.IsInterface, config, relatedIdentityMap, propertyKey);
                }
                else if (p.Value.IsDataEntityList)
                {
                    var elementType = p.Value.ElementType;
                    if (elementType != null)
                    {
                        IIdentityMap relatedIdentityMap = null;
                        if (identityMap != null)
                        {
                            relatedIdentityMap = Identity.Get(elementType, config);
                        }

                        var items = ConvertDataTable(childRows, elementType, elementType.IsInterface, config, relatedIdentityMap);
                        IList list;
                        if (!p.Value.IsListInterface)
                        {
                            list = (IList)p.Key.PropertyType.New();
                        }
                        else
                        {
                            list = List.Create(elementType, p.Value.Distinct, p.Value.Sorted);
                        }

                        foreach (var item in items)
                        {
                            list.Add(item);
                        }
                        
                        propertyValue = list;
                    }
                }
                Reflector.Property.Set(value.GetType(), value, p.Key.Name, propertyValue);
            }
        }

        private static IEnumerable<T> ConvertDataReader<T>(IDataReader reader, Func<object[], T> map, IList<Type> types, bool isInterface, IConfiguration config)
            where T : class
        {
            try
            {
                var isAccumulator = false;
                var count = reader.FieldCount;
                var references = new Dictionary<Tuple<Type, string>, object>();
                while (reader.Read())
                {
                    if (!isInterface || config.DefaultMaterializationMode == MaterializationMode.Exact)
                    {
                        var item = Create<T>(isInterface);
                        Map(reader, item, config.AutoTypeCoercion);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                if (!references.TryGetValue(identity, out var reference))
                                {
                                    reference = Map((object)reader, types[i], config.AutoTypeCoercion);
                                    references.Add(identity, reference);
                                }
                                args[i] = reference;
                            }
                            var mappedItem = map(args);
                            if (mappedItem != null)
                            {
                                yield return mappedItem;
                            }
                            else
                            {
                                isAccumulator = true;
                            }
                        }
                        else
                        {
                            yield return item;
                        }
                    }
                    else
                    {
                        var bag = new Dictionary<string, object>();
                        for (var index = 0; index < count; index++)
                        {
                            bag.Add(reader.GetName(index), reader[index]);
                        }
                        var item = Wrap<T>(bag);

                        TrySetObjectState(item);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                if (!references.TryGetValue(identity, out var reference))
                                {
                                    reference = Wrap(bag, types[i]);
                                    references.Add(identity, reference);
                                }
                                args[i] = reference;
                            }
                            var mappedItem = map(args);
                            if (mappedItem != null)
                            {
                                yield return mappedItem;
                            }
                            else
                            {
                                isAccumulator = true;
                            }
                        }
                        else
                        {
                            yield return item;
                        }
                    }
                }

                // Flush accumulating item
                if (isAccumulator)
                {
                    var mappedItem = map(new object[types.Count]);
                    if (mappedItem != null)
                    {
                        yield return mappedItem;
                    }
                }
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                }
            }
        }

        private static Tuple<Type, string> CreateIdentity(Type objectType, IDataRecord record)
        {
            var nameMap = Reflector.GetPropertyNameMap(objectType);
            var identity = Tuple.Create(objectType, string.Join(",", nameMap.Values.Where(p => p.IsPrimaryKey)
                                                                                .Select(p => p.MappedColumnName ?? p.PropertyName)
                                                                                .OrderBy(_ => _)
                                                                                .Select(n => Convert.ToString(record.GetValue(record.GetOrdinal(n))))));
            return identity;
        }

        private static IEnumerable<ITypeUnion> ConvertDataReaderMultiResult(IDataReader reader, IList<Type> types, bool isInterface, IConfiguration config)
        {
            try
            {
                int resultIndex = 0;
                do
                {
                    var columns = reader.GetColumns();
                    while (reader.Read())
                    {
                        if (!isInterface || config.DefaultMaterializationMode == MaterializationMode.Exact)
                        {
                            var item = Map((object)new WrappedReader(reader, columns), types[resultIndex], config.AutoTypeCoercion);
                            TrySetObjectState(item);
                            yield return TypeUnion.Create(types, item);
                        }
                        else
                        {
                            var bag = new Dictionary<string, object>();
                            for (int index = 0; index < columns.Count; index++)
                            {
                                bag.Add(reader.GetName(index), reader.GetValue(index));
                            }
                            var item = Wrap(bag, types[resultIndex]);
                            TrySetObjectState(item);
                            yield return TypeUnion.Create(types, item);
                        }
                    }
                    resultIndex++;
                    if (resultIndex < types.Count)
                    {
                        isInterface = types[resultIndex].IsInterface;
                    }
                } while (reader.NextResult());
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                }
            }
        }

        internal static void TrySetObjectState(object item, ObjectState state = ObjectState.Clean)
        {
            var entity = item as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = state;
            }
        }
        
        private static IDictionary<string, object> GetSerializableDataRow(DataRow row)
        {
            IDictionary<string, object> result = new Dictionary<string, object>(StringComparer.Ordinal);
            if (row != null)
            {
                foreach (DataColumn column in row.Table.Columns)
                {
                    result.Add(column.ColumnName, row[column]);
                }
            }
            return result;
        }

        #endregion

        #region Helper Methods

        private static void InferRelations(DataSet set, Type objectType, string tableName = null)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            if (tableName == null)
            {
                tableName = GetTableName(objectType);
            }

            var primaryKey = propertyMap.Where(p => p.Value.IsPrimaryKey).OrderBy(p => p.Value.KeyPosition).Select(p => p.Value).ToList();
            var references = propertyMap.Where(p => p.Value.IsDataEntity || p.Value.IsDataEntityList || p.Value.IsObject || p.Value.IsObjectList).Select(p => p.Value);
            foreach (var reference in references)
            {
                var elementType = (reference.IsDataEntityList || reference.IsObjectList) ? reference.ElementType : reference.PropertyType;

                var referencedPropertyMap = Reflector.GetPropertyMap(elementType);
                var referencedProperties = referencedPropertyMap.Where(p => p.Value != null && p.Value.Parent == objectType).OrderBy(p => p.Value.RefPosition).Select(p => p.Value).ToList();
                if (referencedProperties.Count > 0)
                {
                    var referencedTableName = GetTableName(elementType);

                    if (set.Tables.Contains(tableName) && set.Tables.Contains(referencedTableName))
                    {
                        var sourceColumns = primaryKey.Select(p => set.Tables[tableName].Columns[p.MappedColumnName]).ToArray();
                        var targetColumns = referencedProperties.Select(p => set.Tables[referencedTableName].Columns[p.MappedColumnName]).ToArray();
                        var relation = new DataRelation("_" + reference.PropertyName, sourceColumns, targetColumns, false);
                        set.Relations.Add(relation);
                        InferRelations(set, elementType, referencedTableName);
                    }
                }
            }
        }
        
        public static TransactionScope CreateTransactionScope(System.Transactions.IsolationLevel isolationLevel = System.Transactions.IsolationLevel.ReadCommitted)
        {
            var options = new TransactionOptions { IsolationLevel = isolationLevel, Timeout = TimeSpan.MaxValue };
            return new TransactionScope(TransactionScopeOption.Required, options);
        }

        internal static string GetOperationText(Type objectType, string operation, OperationType operationType, string schema, IConfiguration config)
        {
            if (operationType != OperationType.StoredProcedure) return operation;

            var namingConvention = config.OperationNamingConvention;
            var typeName = objectType.Name;
            if (objectType.IsInterface && typeName[0] == 'I')
            {
                typeName = typeName.Substring(1);
            }
            
            var procName = config.OperationPrefix + typeName + "_" + operation;
            switch (namingConvention)
            {
                case OperationNamingConvention.PrefixTypeNameOperation:
                    procName = config.OperationPrefix + typeName + operation;
                    break;
                case OperationNamingConvention.TypeName_Operation:
                    procName = typeName + "_" + operation;
                    break;
                case OperationNamingConvention.TypeNameOperation:
                    procName = typeName + operation;
                    break;
                case OperationNamingConvention.PrefixOperation_TypeName:
                    procName = config.OperationPrefix + operation + "_" + typeName;
                    break;
                case OperationNamingConvention.PrefixOperationTypeName:
                    procName = config.OperationPrefix + operation + typeName;
                    break;
                case OperationNamingConvention.Operation_TypeName:
                    procName = operation + "_" + typeName;
                    break;
                case OperationNamingConvention.OperationTypeName:
                    procName = operation + typeName;
                    break;
                case OperationNamingConvention.PrefixOperation:
                    procName = config.OperationPrefix + operation;
                    break;
                case OperationNamingConvention.Operation:
                    procName = operation;
                    break;
            }

            if (!string.IsNullOrEmpty(schema))
            {
                procName = schema + "." + procName;
            }

            operation = procName;
            return operation;
        }

        internal static string GetTableName<T>()
            where T : class
        {
            string tableName = null;
            
            var map = MappingFactory.GetEntityMap<T>();
            if (map != null)
            {
                tableName = map.TableName;
            }

            if (tableName == null)
            {
                var attr = Reflector.GetAttribute<T, TableAttribute>();
                if (attr != null)
                {
                    tableName = attr.Name;
                }
            }
            
            if (tableName == null)
            {
                var objectType = typeof(T);
                tableName = objectType.Name;
                if (objectType.IsInterface && tableName[0] == 'I')
                {
                    tableName = tableName.Substring(1);
                }
            }
            return tableName;
        }

        internal static string GetTableName(Type objectType)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
            }

            var map = MappingFactory.GetEntityMap(objectType);
            if (map != null)
            {
                tableName = map.TableName;
            }

            if (tableName == null)
            {
                var attr = Reflector.GetAttribute<TableAttribute>(objectType);
                if (attr != null)
                {
                    tableName = attr.Name;
                }
            }

            if (tableName == null)
            {
                tableName = objectType.Name;
                if (objectType.IsInterface && tableName[0] == 'I')
                {
                    tableName = tableName.Substring(1);
                }
            }
            return tableName;
        }

        internal static string GetUnmappedTableName(Type objectType)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
            }

            var attr = Reflector.GetAttribute<TableAttribute>(objectType);
            if (attr != null)
            {
                tableName = attr.Name;
            }

            if (tableName == null)
            {
                tableName = objectType.Name;
                if (objectType.IsInterface && tableName[0] == 'I')
                {
                    tableName = tableName.Substring(1);
                }
            }
            return tableName;
        }

        public static string[] GetPrimaryKeyProperties(Type objectType)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            return propertyMap.Values.Where(p => p.CanRead && p.IsPrimaryKey).Select(p => p.PropertyName).ToArray();
        }

        private static string[] GetPrimaryKeyColumns(Type objectType)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            return propertyMap.Values.Where(p => p.CanRead && p.IsPrimaryKey).Select(p => p.MappedColumnName ?? p.PropertyName).ToArray();
        }

        #endregion
    }
}
