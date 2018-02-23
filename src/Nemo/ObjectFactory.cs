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

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle?> CreateMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle?>();

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

            var genericCreateMethod = CreateMethods.GetOrAdd(targetType, type =>
            {
                var createMethod = typeof(ObjectFactory).GetMethods().FirstOrDefault(m => m.Name == "Create" && m.GetGenericArguments().Length == 1 && m.GetParameters().Length == 1);
                if (createMethod != null)
                {
                    return createMethod.MakeGenericMethod(targetType).MethodHandle;
                }
                return null;
            });

            if (genericCreateMethod == null) return null;

            var mapDelegate = Reflector.Method.CreateDelegate(genericCreateMethod.Value);
            return mapDelegate(null, new object[] { targetType.IsInterface });
        }

        #endregion

        #region Map Methods

        private static readonly ConcurrentDictionary<Tuple<Type, Type, bool>, RuntimeMethodHandle?> MapMethods = new ConcurrentDictionary<Tuple<Type, Type, bool>, RuntimeMethodHandle?>();

        public static object Map(object source, Type targetType, bool ignoreMappings = false)
        {
            return Map(source, targetType, targetType.IsInterface, ignoreMappings);
        }

        internal static object Map(object source, Type targetType, bool isInterface, bool ignoreMappings)
        {
            if (source == null) return null;

            var instanceType = source.GetType();
            var key = Tuple.Create(instanceType, targetType, ignoreMappings);
            var genericMapMethodHandle = MapMethods.GetOrAdd(key, t =>
            {
                if (!Reflector.IsAnonymousType(instanceType))
                {
                    var mapMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).FirstOrDefault(m => m.Name == "Map" && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 3);
                    if (mapMethod != null)
                    {
                        return mapMethod.MakeGenericMethod(t.Item1, t.Item2).MethodHandle;
                    }
                }
                return null;
            });

            if (genericMapMethodHandle == null) return null;

            var mapDelegate = Reflector.Method.CreateDelegate(genericMapMethodHandle.Value);
            return mapDelegate(null, new[] { source, isInterface, ignoreMappings });
        }

        internal static TResult Map<TSource, TResult>(TSource source, bool isInterface, bool ignoreMappings)
            where TResult : class
            where TSource : class
        {
            var target = Create<TResult>(isInterface);
            return Map(source, target, ignoreMappings);
        }

        public static TResult Map<TSource, TResult>(TSource source, bool ignoreMappings = false)
            where TResult : class
            where TSource : class
        {
            return Map<TSource, TResult>(source, typeof(TResult).IsInterface, ignoreMappings);
        }

        public static TResult Map<TSource, TResult>(TSource source, TResult target, bool ignoreMappings = false)
            where TResult : class
            where TSource : class
        {
            var indexer = source is IDictionary<string, object> || (source is IDataRecord) || source is DataRow;

            if (indexer)
            {
                if (ignoreMappings)
                {
                    FastExactIndexerMapper<TSource, TResult>.Map(source, target);
                }
                else
                {
                    var reader = source as IDataReader;
                    if (reader != null)
                    {
                       FastIndexerMapper<IDataRecord, TResult>.Map(reader, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                if (ignoreMappings)
                {
                    FastExactMapper<TSource, TResult>.Map(source, target);
                }
                else
                {
                    FastMapper<TSource, TResult>.Map(source, target);
                }
            }
            return target;
        }

        public static T Map<T>(IDictionary<string, object> source, T target, bool ignoreMappings = false)
            where T : class
        {
            if (ignoreMappings)
            {
                FastExactIndexerMapper<IDictionary<string, object>, T>.Map(source, target);
            }
            else
            {
                FastIndexerMapper<IDictionary<string, object>, T>.Map(source, target);
            }
            return target;
        }

        public static T Map<T>(DataRow source, T target, bool ignoreMappings = false)
            where T : class
        {
            if (ignoreMappings)
            {
                FastExactIndexerMapper<DataRow, T>.Map(source, target);
            }
            else
            {
                FastIndexerMapper<DataRow, T>.Map(source, target);
            }
            return target;
        }

        public static T Map<T>(IDataReader source, T target, bool ignoreMappings = false)
            where T : class
        {
            if (ignoreMappings)
            {
                FastExactIndexerMapper<IDataRecord, T>.Map(source, target);
            }
            else
            {
                FastIndexerMapper<IDataRecord, T>.Map(source, target);
            }
            return target;
        }

        /// <summary>
        /// Maps values from source to target by copying object's properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="ignoreMappings"></param>
        /// <returns></returns>
        public static T Map<T>(object source, bool ignoreMappings = false)
            where T : class
        {
            return (T)Map(source, typeof(T), ignoreMappings);
        }

        #endregion

        #region Bind Methods

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle?> _bindMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle?>();

        /// <summary>
        /// Binds interface implementation to the existing object type.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static TResult Bind<TSource, TResult>(TSource source)
            where TResult : class
            where TSource : class
        {
            if (source == null) throw new ArgumentNullException("source");
            var target = Adapter.Bind<TResult>(source);
            return target;
        }

        /// <summary>
        /// Binds interface implementation to the existing object type. 
        /// This method uses reflection to invoke generic implementation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static T Bind<T>(object source)
            where T : class
        {
            if (source == null) return null;

            var instanceType = source.GetType();
            if (Reflector.IsAnonymousType(instanceType))
            {
                return Adapter.Bind<T>(source);
            }
            var genericBindMethod = _bindMethods.GetOrAdd(instanceType, type =>
            {
                var bindMethod = typeof(ObjectFactory).GetMethods().FirstOrDefault(m => m.Name == "Bind" && m.GetGenericArguments().Length == 2);
                if (bindMethod != null)
                {
                    return bindMethod.MakeGenericMethod(type, typeof(T)).MethodHandle;
                }
                return null;
            });

            if (genericBindMethod == null) return null;

            var bindDelegate = Reflector.Method.CreateDelegate(genericBindMethod.Value);
            return (T)bindDelegate(null, new[] { source });
        }

        #endregion

        #region Wrap Methods

        /// <summary>
        /// Converts a dictionary to an instance of the object specified by the interface.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="map"></param>
        /// <param name="ignoreMappings"></param>
        /// <param name="includeAllProperties"></param>
        /// <returns></returns>
        public static T Wrap<T>(IDictionary<string, object> map, bool ignoreMappings = false, bool includeAllProperties = false)
             where T : class
        {
            ObjectActivator activator;
            if (ignoreMappings)
            {
                activator = includeAllProperties ? FastExactComplexWrapper<T>.Instance : FastExactWrapper<T>.Instance;
            }
            else
            {
                activator = includeAllProperties ? FastComplexWrapper<T>.Instance : FastWrapper<T>.Instance;
            }
            return (T)activator(map);
        }

        public static object Wrap(IDictionary<string, object> value, Type targetType, bool ignoreMappings = false, bool includeAllProperties = false)
        {
            return Adapter.Wrap(value, targetType, ignoreMappings, includeAllProperties);
        }

        #endregion

        #region Count Methods

        public static int Count<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }
            var sql = SqlBuilder.GetSelectCountStatement(predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<int>(sql, connection: connection);
        }

        #endregion

        #region Select Methods

        public static IEnumerable<T> Select<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, int page = 0, int pageSize = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, params Sorting<T>[] orderBy)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }

            var provider = DialectFactory.GetProvider(connection, providerName);

            var sql = SqlBuilder.GetSelectStatement(predicate, page, pageSize, selectOption != SelectOption.All, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sql }, new[] { typeof(T) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.SingleResult, connectionName, connection, types: t, cached: cached), predicate, provider, selectOption);

            return result;
        }

        private static IEnumerable<T> Select<T, T1>(Expression<Func<T, T1, bool>> join, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0,
            bool? cached = null, SelectOption selectOption = SelectOption.All, params Sorting<T>[] orderBy)
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

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin = SqlBuilder.GetSelectStatement(predicate, join, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin }, new[] { typeof(T), typeof(T1) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached), predicate, provider, selectOption);

            return result;
        }
        
        private static IEnumerable<T> Select<T, T1, T2>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, bool? cached = null, 
            SelectOption selectOption = SelectOption.All, params Sorting<T>[] orderBy)
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

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2 }, new[] { typeof(T), typeof(T1), typeof(T2) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached), predicate, provider, selectOption);

            return result;
        }

        internal static IEnumerable<T> Select<T, T1, T2, T3>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, bool? cached = null,
            SelectOption selectOption = SelectOption.All, params Sorting<T>[] orderBy)
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

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, false, provider, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3) },
                (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached), predicate, provider, selectOption);

            return result;
        }

        private static IEnumerable<T> Select<T, T1, T2, T3, T4>(Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, DialectProvider provider = null, int page = 0, int pageSize = 0, bool? cached = null, 
            SelectOption selectOption = SelectOption.All, params Sorting<T>[] orderBy)
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

            var sqlRoot = SqlBuilder.GetSelectStatement(predicate, page, pageSize, selectOption != SelectOption.All, provider, orderBy);
            var sqlJoin1 = SqlBuilder.GetSelectStatement(predicate, join1, 0, 0, false, provider, orderBy);
            var sqlJoin2 = SqlBuilder.GetSelectStatement(predicate, join1, join2, 0, 0, false, provider, orderBy);
            var sqlJoin3 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, 0, 0, false, provider, orderBy);
            var sqlJoin4 = SqlBuilder.GetSelectStatement(predicate, join1, join2, join3, join4, 0, 0, false, provider, orderBy);

            var result = new EagerLoadEnumerable<T>(new[] { sqlRoot, sqlJoin1, sqlJoin2, sqlJoin3, sqlJoin4 }, new[] { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) },
              (s, t) => RetrieveImplemenation<T>(s, OperationType.Sql, null, OperationReturnType.MultiResult, connectionName, connection, types: t, cached: cached), predicate, provider, selectOption);

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
            var eagerSource = source as EagerLoadEnumerable<TSource>;
            if (eagerSource != null)
            {
                return Union(source, Select(join, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption));
            }
            return source;
        }

        public static IEnumerable<TSource> Include<TSource, TInclude1, TInclude2>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
        {
            var eagerSource = source as EagerLoadEnumerable<TSource>;
            if (eagerSource != null)
            {
                return Union(source, Select(join1, join2, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption));
            }
            return source;
        }

        public static IEnumerable<TSource> Include<TSource, TInclude1, TInclude2, TInclude3>(this IEnumerable<TSource> source, Expression<Func<TSource, TInclude1, bool>> join1, Expression<Func<TInclude1, TInclude2, bool>> join2, Expression<Func<TInclude2, TInclude3, bool>> join3)
            where TSource : class
            where TInclude1 : class
            where TInclude2 : class
            where TInclude3 : class
        {
            var eagerSource = source as EagerLoadEnumerable<TSource>;
            if (eagerSource != null)
            {
                return Union(source, Select(join1, join2, join3, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption));
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
            var eagerSource = source as EagerLoadEnumerable<TSource>;
            if (eagerSource != null)
            {
                return Union(source, Select(join1, join2, join3, join4, eagerSource.Predicate, provider: eagerSource.Provider, selectOption: eagerSource.SelectOption));
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
            Log.CaptureBegin(() => string.Format("RetrieveImplemenation: {0}::{1}", typeof(TResult).FullName, operation));
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

                Log.CaptureBegin(() => string.Format("Retrieving from L1 cache: {0}", queryKey));

                if (returnType == OperationReturnType.MultiResult)
                {
                    result = config.ExecutionContext.Get(queryKey) as IEnumerable<TResult>;
                }
                else
                {
                    identityMap = Identity.Get<TResult>();
                    result = identityMap.GetIndex(queryKey);
                }
                
                Log.CaptureEnd();

                if (result != null)
                {
                    Log.Capture(() => string.Format("Found in L1 cache: {0}", queryKey));
                    
                    if (returnType == OperationReturnType.MultiResult)
                    {
                        ((IMultiResult)result).Reset();
                    }
                    
                    Log.CaptureEnd();
                    return result;
                }
                Log.Capture(() => string.Format("Not found in L1 cache: {0}", queryKey));
            }

            result = RetrieveItems(operation, parameters, operationType, returnType, connectionName, connection, types, map, cached.Value, schema, config, identityMap);
            
            if (queryKey != null)
            {
                Log.CaptureBegin(() => string.Format("Saving to L1 cache: {0}", queryKey));

                if (!(result is IList<TResult>) && !(result is IMultiResult))
                {
                    if (config.DefaultCacheRepresentation == CacheRepresentation.List)
                    {
                        result = result.ToList();
                    }
                    else
                    {
                        result = result.AsStream();
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
            var mode = config.DefaultMaterializationMode;

            var result = Translate(response, map, types, cached, mode, returnType, identityMap);
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
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                var list = parameters as ParamList;
                if (list != null)
                {
                    parameterList = list.GetParameters(typeof(TResult), operation);
                }
                else
                {
                    var array = parameters as Param[];
                    if (array != null)
                    {
                        parameterList = array;
                    }
                }
            }
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
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                var list = parameters as ParamList;
                if (list != null)
                {
                    parameterList = list.GetParameters(typeof(T), operation);
                }
                else
                {
                    var array = parameters as Param[];
                    if (array != null)
                    {
                        parameterList = array;
                    }
                }
            }
            return RetrieveImplemenation<T>(command, commandType, parameterList, OperationReturnType.SingleResult, connectionName, connection, null, new[] { typeof(T) }, schema, cached, config);
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
                foreach (var item in batch)
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

        public static long Insert<T>(IEnumerable<T> items, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false)
            where T : class
        {
            var count = 0L;
            var connectionOpenedHere = false;
            var externalTransaction = transaction != null;
            var externalConnection = externalTransaction || connection != null;
            var config = ConfigurationFactory.Get<T>();
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
                if (transaction != null)
                {
                    transaction.Rollback();
                }
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

        public static OperationResponse Insert<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            return Insert<T>(parameters.GetParameters(typeof(T), OperationInsert), connectionName, captureException, schema, connection);
        }

        public static OperationResponse Insert<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            var config = ConfigurationFactory.Get<T>();
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

        public static OperationResponse Update<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            return Update<T>(parameters.GetParameters(typeof(T), OperationUpdate), connectionName, captureException, schema, connection);
        }

        public static OperationResponse Update<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            var config = ConfigurationFactory.Get<T>();
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
                        string column;
                        if (pimaryKeySet.TryGetValue(p.Name, out column))
                        {
                            p.Source = column;
                            p.IsPrimaryKey = true;
                            return true;
                        }
                        return false;
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

        public static OperationResponse Delete<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            return Delete<T>(parameters.GetParameters(typeof(T), OperationDelete), connectionName, captureException, schema, connection);
        }

        public static OperationResponse Delete<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            var config = ConfigurationFactory.Get<T>();
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
                        string column;
                        if (pimaryKeySet.TryGetValue(p.Name, out column))
                        {
                            p.Source = column;
                            p.IsPrimaryKey = true;
                            return true;
                        }
                        return false;
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

        public static OperationResponse Destroy<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            return Destroy<T>(parameters.GetParameters(typeof(T), OperationDestroy), connectionName, captureException, schema, connection);
        }

        public static OperationResponse Destroy<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException };
            var config = ConfigurationFactory.Get<T>();
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
            var rootType = types != null ? types[0] : null;

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

                        dbParam.DbType = !parameter.DbType.HasValue ? Reflector.ClrToDbType(parameter.Type) : parameter.DbType.Value;
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

        public static IEnumerable<T> Translate<T>(OperationResponse response)
            where T : class
        {
            var config = ConfigurationFactory.Get<T>();
            return Translate<T>(response, null, null, config.DefaultCacheRepresentation != CacheRepresentation.None, config.DefaultMaterializationMode, response.ReturnType, Identity.Get<T>());
        }

        private static IEnumerable<T> Translate<T>(OperationResponse response, Func<object[], T> map, IList<Type> types, bool cached, MaterializationMode mode, OperationReturnType returnType, IIdentityMap identityMap)
            where T : class
        {
            var value = response != null ? response.Value : null;
            if (value == null)
            {
                return Enumerable.Empty<T>();
            }

            var isInterface = Reflector.GetReflectedType<T>().IsInterface;

            var reader = value as IDataReader;
            if (reader != null)
            {
                if (map == null && types != null && types.Count > 1)
                {
                    var multiResultItems = ConvertDataReaderMultiResult(reader, types, mode, isInterface);
                    return (IEnumerable<T>)MultiResult.Create(types, multiResultItems, cached);
                }
                return ConvertDataReader(reader, map, types, mode, isInterface);
            }

            var dataSet = value as DataSet;
            if (dataSet != null)
            {
                return ConvertDataSet<T>(dataSet, mode, isInterface, identityMap);
            }

            var dataTable = value as DataTable;
            if (dataTable != null)
            {
                return ConvertDataTable<T>(dataTable, mode, isInterface, identityMap);
            }

            var dataRow = value as DataRow;
            if (dataRow != null)
            {
                return ConvertDataRow<T>(dataRow, mode, isInterface, identityMap, null).Return();
            }

            var item = value as T;
            if (item != null)
            {
                return item.Return();
            }

            var genericList = value as IList<T>;
            if (genericList != null)
            {
                return genericList;
            }

            var list = value as IList;
            if (list != null)
            {
                return list.Cast<object>().Select(i => mode == MaterializationMode.Exact ? Map<T>(i) : Bind<T>(i));
            }
            
            return Bind<T>(value).Return();
        }

        private static IEnumerable<T> ConvertDataSet<T>(DataSet dataSet, MaterializationMode mode, bool isInterface, IIdentityMap identityMap)
             where T : class
        {
            var tableName = GetTableName(typeof(T));
            return dataSet.Tables.Count != 0 ? ConvertDataTable<T>(dataSet.Tables.Contains(tableName) ? dataSet.Tables[tableName] : dataSet.Tables[0], mode, isInterface, identityMap) : Enumerable.Empty<T>();
        }

        private static IEnumerable<T> ConvertDataTable<T>(DataTable table, MaterializationMode mode, bool isInterface, IIdentityMap identityMap)
             where T : class
        {
            return ConvertDataTable<T>(table.Rows.Cast<DataRow>(), mode, isInterface, identityMap);
        }

        private static IEnumerable<T> ConvertDataTable<T>(IEnumerable<DataRow> table, MaterializationMode mode, bool isInterface, IIdentityMap identityMap)
             where T : class
        {
            var primaryKey = GetPrimaryKeyColumns(typeof(T));
            return table.Select(row => ConvertDataRow<T>(row, mode, isInterface, identityMap, primaryKey));
        }

        private static IEnumerable<object> ConvertDataTable(IEnumerable<DataRow> table, Type targetType, MaterializationMode mode, bool isInterface, IIdentityMap identityMap)
        {
            var primaryKey = GetPrimaryKeyColumns(targetType);
            return table.Select(row => ConvertDataRow(row, targetType, mode, isInterface, identityMap, primaryKey));
        }
        
        private static T ConvertDataRow<T>(DataRow row, MaterializationMode mode, bool isInterface, IIdentityMap identityMap, string[] primaryKey)
             where T : class
        {
            var result = identityMap.GetEntityByKey<DataRow, T>(row.GetKeySelector(primaryKey), out string hash);

            if (result != null) return result;

            var value = mode == MaterializationMode.Exact || !isInterface ? Map<DataRow, T>(row, isInterface) : Wrap<T>(GetSerializableDataRow(row));
            var entity = value as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.Clean;
            }

            // Write-through for identity map
            identityMap.WriteThrough(value, hash);

            LoadRelatedData(row, value, typeof(T), mode, identityMap, primaryKey);
            
            return value;
        }

        private static object ConvertDataRow(DataRow row, Type targetType, MaterializationMode mode, bool isInterface, IIdentityMap identityMap, string[] primaryKey)
        {
            string hash = null;

            if (identityMap != null)
            {
                var primaryKeyValue = new SortedDictionary<string, object>(primaryKey.ToDictionary(k => k, k => row[k]), StringComparer.Ordinal);
                hash = primaryKeyValue.ComputeHash(targetType);

                object result;
                if (identityMap.TryGetValue(hash, out result))
                {
                    return result;
                }
            }

            var value = mode == MaterializationMode.Exact || !isInterface ? Map((object)row, targetType, isInterface) : Wrap(GetSerializableDataRow(row), targetType);
            var entity = value as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.Clean;
            }

            // Write-through for identity map
            if (identityMap != null && value != null && hash != null)
            {
                identityMap.Set(hash, value);
            }
            
            LoadRelatedData(row, value, targetType, mode, identityMap, primaryKey);

            return value;
        }

        private static void LoadRelatedData(DataRow row, object value, Type targetType, MaterializationMode mode, IIdentityMap identityMap, string[] primaryKey)
        {
            var table = row.Table;
            if (table == null || table.ChildRelations.Count <= 0) return;

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
                        relatedIdentityMap = Identity.Get(p.Key.PropertyType);
                    }
                    propertyValue = ConvertDataRow(childRows[0], p.Key.PropertyType, mode, p.Key.PropertyType.IsInterface, relatedIdentityMap, propertyKey);
                }
                else if (p.Value.IsDataEntityList)
                {
                    var elementType = p.Value.ElementType;
                    if (elementType != null)
                    {
                        IIdentityMap relatedIdentityMap = null;
                        if (identityMap != null)
                        {
                            relatedIdentityMap = Identity.Get(elementType);
                        }

                        var items = ConvertDataTable(childRows, elementType, mode, elementType.IsInterface, relatedIdentityMap);
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

        private static IEnumerable<T> ConvertDataReader<T>(IDataReader reader, Func<object[], T> map, IList<Type> types, MaterializationMode mode, bool isInterface)
            where T : class
        {
            try
            {
                var isAccumulator = false;
                var count = reader.FieldCount;
                var references = new Dictionary<Tuple<Type, string>, object>();
                while (reader.Read())
                {
                    if (!isInterface || mode == MaterializationMode.Exact)
                    {
                        var item = Create<T>(isInterface);
                        Map(reader, item, false);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                object reference;
                                if (!references.TryGetValue(identity, out reference))
                                {
                                    reference = Map((object)reader, types[i]);
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
                                object reference;
                                if (!references.TryGetValue(identity, out reference))
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

        private static IEnumerable<ITypeUnion> ConvertDataReaderMultiResult(IDataReader reader, IList<Type> types, MaterializationMode mode, bool isInterface)
        {
            try
            {
                int resultIndex = 0;
                do
                {
                    int count = reader.FieldCount;
                    while (reader.Read())
                    {
                        if (!isInterface || mode == MaterializationMode.Exact)
                        {
                            var item = Map(reader, types[resultIndex], isInterface, false);
                            TrySetObjectState(item);
                            yield return TypeUnion.Create(types, item);
                        }
                        else
                        {
                            var bag = new Dictionary<string, object>();
                            for (int index = 0; index < count; index++)
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

        private static void TrySetObjectState(object item)
        {
            var entity = item as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.Clean;
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
