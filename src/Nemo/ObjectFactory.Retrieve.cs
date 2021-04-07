using Nemo.Collections;
using Nemo.Configuration;
using Nemo.Fn;
using Nemo.Fn.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Retrieve Methods

        public static T RetrieveScalar<T>(string sql, Param[] parameters = null, string connectionName = null, DbConnection connection = null, string schema = null, IConfiguration config = null)
            where T : struct
        {
            var response = connection != null
                ? Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connection: connection, schema: schema, config: config)
                : Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connectionName: connectionName, schema: schema, config: config);

            var value = response.Value;
            if (value == null)
            {
                return default;
            }

            return (T)Reflector.ChangeType(value, typeof(T));
        }

        private static IEnumerable<TResult> RetrieveImplemenation<TResult>(string operation, OperationType operationType, IList<Param> parameters, OperationReturnType returnType, string connectionName, DbConnection connection, Func<object[], TResult> map = null, IList<Type> types = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where TResult : class
        {
            Log.CaptureBegin($"Retrieve {typeof(TResult).FullName}", config);
            IEnumerable<TResult> result;

            string queryKey = null;
            IdentityMap<TResult> identityMap = null;

            if (!cached.HasValue)
            {
                config ??= ConfigurationFactory.Get<TResult>();

                cached = config.DefaultCacheRepresentation != CacheRepresentation.None;
            }

            if (cached.Value)
            {
                config ??= ConfigurationFactory.Get<TResult>();

                queryKey = GetQueryKey<TResult>(operation, parameters ?? new Param[] { }, returnType);

                Log.CaptureBegin($"Retrieving from L1 cache: {queryKey}", config);

                if (returnType == OperationReturnType.MultiResult)
                {
                    result = config.ExecutionContext.Get(queryKey) as IEnumerable<TResult>;
                }
                else
                {
                    identityMap = Identity.Get<TResult>(config);
                    result = identityMap.GetIndex(queryKey);
                }

                Log.CaptureEnd(config);

                if (result != null)
                {
                    Log.Capture($"Found in L1 cache: {queryKey}", config);

                    if (returnType == OperationReturnType.MultiResult)
                    {
                        ((IMultiResult)result).Reset();
                    }

                    Log.CaptureEnd(config);
                    return result;
                }
                Log.Capture($"Not found in L1 cache: {queryKey}", config);
            }

            result = RetrieveItems(operation, parameters, operationType, returnType, connectionName, connection, types, map, cached.Value, schema, config, identityMap);

            if (queryKey != null)
            {
                Log.CaptureBegin($"Saving to L1 cache: {queryKey}", config);

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

                Log.CaptureEnd(config);
            }

            Log.CaptureEnd(config);
            return result;
        }

        private static IEnumerable<T> RetrieveItems<T>(string operation, IList<Param> parameters, OperationType operationType, OperationReturnType returnType, string connectionName, DbConnection connection, IList<Type> types, Func<object[], T> map, bool cached, string schema, IConfiguration config, IdentityMap<T> identityMap)
            where T : class
        {
            if (operationType == OperationType.Guess)
            {
                operationType = operation.Any(char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            config ??= ConfigurationFactory.Get<T>();

            var operationText = GetOperationText(typeof(T), operation, operationType, schema, config);

            Log.CaptureBegin($"Retrieve data from database using {operationText}", config);

            var response = connection != null
                ? Execute(operationText, parameters, returnType, connection: connection, operationType: operationType, types: types, schema: schema, config: config)
                : Execute(operationText, parameters, returnType, connectionName: connectionName, operationType: operationType, types: types, schema: schema, config: config);

            Log.CaptureEnd(config);

            Log.CaptureBegin($"Translating response", config);

            var result = Translate(response, map, types, config, identityMap);

            Log.CaptureEnd(config);

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

            var typeCount = 1;
            typeCount += LoadTypes<T1>(fakeType, realTypes, out var hasTuple) ? 1 : 0;
            typeCount += LoadTypes<T2>(fakeType, realTypes, out hasTuple) ? 1 : 0;
            typeCount += LoadTypes<T3>(fakeType, realTypes, out hasTuple) ? 1 : 0;
            typeCount += LoadTypes<T4>(fakeType, realTypes, out hasTuple) ? 1 : 0;

            config ??= ConfigurationFactory.Get<TResult>();

            var returnType = OperationReturnType.SingleResult;

            Func<object[], TResult> func = null;
            if (map == null && realTypes.Count > 1)
            {
                returnType = OperationReturnType.MultiResult;
            }
            else if (map != null && typeCount > 1 && typeCount < 6 && !hasTuple)
            {
                switch (typeCount)
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
            var parameterList = ExtractParameters<TResult>(parameters);
            return RetrieveImplemenation(command, commandType, parameterList, returnType, connectionName, connection, func, realTypes, schema, cached, config);
        }

        private static bool LoadTypes<T>(Type fakeType, List<Type> realTypes, out bool hasTuple)
            where T : class
        {
            hasTuple = false;
            if (fakeType != typeof(T))
            {
                if (Reflector.IsTuple(typeof(T)))
                {
                    realTypes.AddRange(Reflector.GetTupleTypes(typeof(T)));
                    hasTuple = true;
                }
                else
                {
                    realTypes.Add(typeof(T));
                }
                return true;
            }
            return false;
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
            config ??= ConfigurationFactory.Get<T>();

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            var parameterList = ExtractParameters<T>(parameters);
            return RetrieveImplemenation<T>(command, commandType, parameterList, OperationReturnType.SingleResult, connectionName, connection, null, new[] { typeof(T) }, schema, cached, config);
        }

        private static IList<Param> ExtractParameters<T>(object parameters)
            where T : class
        {
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                switch (parameters)
                {
                    case ParamList list:
                        parameterList = list.GetParameters();
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

        #region Retrieve Async Methods

        public static async Task<T> RetrieveScalarAsync<T>(string sql, Param[] parameters = null, string connectionName = null, DbConnection connection = null, string schema = null, IConfiguration config = null)
            where T : struct
        {
            var response = connection != null
                ? await ExecuteAsync(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connection: connection, schema: schema, config: config).ConfigureAwait(false)
                : await ExecuteAsync(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connectionName: connectionName, schema: schema, config: config).ConfigureAwait(false);

            var value = response.Value;
            if (value == null)
            {
                return default;
            }

            return (T)Reflector.ChangeType(value, typeof(T));
        }

        private static async Task<IEnumerable<TResult>> RetrieveImplemenationAsync<TResult>(string operation, OperationType operationType, IList<Param> parameters, OperationReturnType returnType, string connectionName, DbConnection connection, Func<object[], TResult> map = null, IList<Type> types = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where TResult : class
        {
            Log.CaptureBegin($"Retrieve {typeof(TResult).FullName}", config);
            IEnumerable<TResult> result;

            string queryKey = null;
            IdentityMap<TResult> identityMap = null;

            if (!cached.HasValue)
            {
                config ??= ConfigurationFactory.Get<TResult>();

                cached = config.DefaultCacheRepresentation != CacheRepresentation.None;
            }

            if (cached.Value)
            {
                config ??= ConfigurationFactory.Get<TResult>();

                queryKey = GetQueryKey<TResult>(operation, parameters ?? new Param[] { }, returnType);

                Log.CaptureBegin($"Retrieving from L1 cache: {queryKey}", config);

                if (returnType == OperationReturnType.MultiResult)
                {
                    result = config.ExecutionContext.Get(queryKey) as IEnumerable<TResult>;
                }
                else
                {
                    identityMap = Identity.Get<TResult>(config);
                    result = identityMap.GetIndex(queryKey);
                }

                Log.CaptureEnd(config);

                if (result != null)
                {
                    Log.Capture($"Found in L1 cache: {queryKey}", config);

                    if (returnType == OperationReturnType.MultiResult)
                    {
                        ((IMultiResult)result).Reset();
                    }

                    Log.CaptureEnd(config);
                    return result;
                }
                Log.Capture($"Not found in L1 cache: {queryKey}", config);
            }

            result = await RetrieveItemsAsync(operation, parameters, operationType, returnType, connectionName, connection, types, map, cached.Value, schema, config, identityMap).ConfigureAwait(false);

            if (queryKey != null)
            {
                Log.CaptureBegin($"Saving to L1 cache: {queryKey}", config);

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

                Log.CaptureEnd(config);
            }

            Log.CaptureEnd(config);
            return result;
        }

        private static async Task<IEnumerable<T>> RetrieveItemsAsync<T>(string operation, IList<Param> parameters, OperationType operationType, OperationReturnType returnType, string connectionName, DbConnection connection, IList<Type> types, Func<object[], T> map, bool cached, string schema, IConfiguration config, IdentityMap<T> identityMap)
            where T : class
        {
            if (operationType == OperationType.Guess)
            {
                operationType = operation.Any(char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            config ??= ConfigurationFactory.Get<T>();

            var operationText = GetOperationText(typeof(T), operation, operationType, schema, config);

            Log.CaptureBegin($"Retrieve data from database using {operationText}", config);

            var response = connection != null
                ? await ExecuteAsync(operationText, parameters, returnType, connection: connection, operationType: operationType, types: types, schema: schema, config: config).ConfigureAwait(false)
                : await ExecuteAsync(operationText, parameters, returnType, connectionName: connectionName ?? config?.DefaultConnectionName, operationType: operationType, types: types, schema: schema, config: config).ConfigureAwait(false);

            Log.CaptureEnd(config);

            Log.CaptureBegin($"Translating response", config);

            var result = Translate(response, map, types, config, identityMap);

            Log.CaptureEnd(config);

            return result;
        }

        /// <summary>
        /// Retrieves an enumerable of type T using provided rule parameters.
        /// </summary>
        /// <returns></returns>
        public static async Task<IEnumerable<TResult>> RetrieveAsync<TResult, T1, T2, T3, T4>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, T4, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
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

            config ??= ConfigurationFactory.Get<TResult>();

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
            var parameterList = ExtractParameters<TResult>(parameters);
            return await RetrieveImplemenationAsync(command, commandType, parameterList, returnType, connectionName, connection, func, realTypes, schema, cached, config).ConfigureAwait(false);
        }

        public static async Task<IEnumerable<TResult>> RetrieveAsync<TResult, T1, T2, T3>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where T2 : class
            where T3 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, t3, f4) => map(t, t1, t2, t3) : (Func<TResult, T1, T2, T3, Fake, TResult>)null;
            return await RetrieveAsync(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config).ConfigureAwait(false);
        }

        public static async Task<IEnumerable<TResult>> RetrieveAsync<TResult, T1, T2>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where T2 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, f3, f4) => map(t, t1, t2) : (Func<TResult, T1, T2, Fake, Fake, TResult>)null;
            return await RetrieveAsync(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config).ConfigureAwait(false);
        }

        public static async Task<IEnumerable<TResult>> RetrieveAsync<TResult, T1>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, TResult> map = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T1 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, f1, f2, f3) => map(t, t1) : (Func<TResult, T1, Fake, Fake, Fake, TResult>)null;
            return await RetrieveAsync(operation, sql, parameters, newMap, connectionName, connection, schema, cached, config).ConfigureAwait(false);
        }

        public static async Task<IEnumerable<T>> RetrieveAsync<T>(string operation = OperationRetrieve, string sql = null, object parameters = null, string connectionName = null, DbConnection connection = null, string schema = null, bool? cached = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            var parameterList = ExtractParameters<T>(parameters);
            return await RetrieveImplemenationAsync<T>(command, commandType, parameterList, OperationReturnType.SingleResult, connectionName, connection, null, new[] { typeof(T) }, schema, cached, config).ConfigureAwait(false);
        }

        #endregion
    }
}
