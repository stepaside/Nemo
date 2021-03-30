using Nemo.Configuration;
using Nemo.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Aggregate Methods

        public static int Count<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Count<T, int>(predicate, connectionName, connection, config);
        }

        public static long LongCount<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
        {
            return Count<T, long>(predicate, connectionName, connection, config);
        }

        internal static TResult Count<T, TResult>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
            where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }
            var sql = SqlBuilder.GetSelectCountStatement(predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<TResult>(sql, connection: connection, config: config);
        }

        public static TResult Max<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.MAX, projection, predicate, connectionName, connection, config);
        }

        public static TResult Min<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.MIN, projection, predicate, connectionName, connection, config);
        }

        public static TResult Sum<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.SUM, projection, predicate, connectionName, connection, config);
        }

        public static TResult Average<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return Aggregate(AggregateNames.AVG, projection, predicate, connectionName, connection, config);
        }

        internal enum AggregateNames { MAX, MIN, SUM, AVG }

        internal static TResult Aggregate<T, TResult>(AggregateNames aggregateName, Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }
            var sql = SqlBuilder.GetSelectAggregationStatement(aggregateName.ToString(), projection, predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<TResult>(sql, connection: connection, config: config);
        }

        #endregion

        #region Aggregate Async Methods

        public static Task<int> CountAsync<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return CountAsync<T, int>(predicate, connectionName, connection, config);
        }

        public static Task<long> LongCountAsync<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return CountAsync<T, long>(predicate, connectionName, connection, config);
        }

        internal static Task<TResult> CountAsync<T, TResult>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
            where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }
            var sql = SqlBuilder.GetSelectCountStatement(predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalarAsync<TResult>(sql, connection: connection, config: config);
        }

        public static Task<TResult> MaxAsync<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return AggregateAsync(AggregateNames.MAX, projection, predicate, connectionName, connection, config);
        }

        public static Task<TResult> MinAsync<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return AggregateAsync(AggregateNames.MIN, projection, predicate, connectionName, connection, config);
        }

        public static Task<TResult> SumAsync<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return AggregateAsync(AggregateNames.SUM, projection, predicate, connectionName, connection, config);
        }

        public static Task<TResult> AverageAsync<T, TResult>(Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            return AggregateAsync(AggregateNames.AVG, projection, predicate, connectionName, connection, config);
        }

        internal static Task<TResult> AggregateAsync<T, TResult>(AggregateNames aggregateName, Expression<Func<T, TResult>> projection, Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, IConfiguration config = null)
           where T : class
           where TResult : struct
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T), config);
                connection = DbFactory.CreateConnection(connectionName, typeof(T), config);
            }
            var sql = SqlBuilder.GetSelectAggregationStatement(aggregateName.ToString(), projection, predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalarAsync<TResult>(sql, connection: connection, config: config);
        }

        #endregion
    }
}
