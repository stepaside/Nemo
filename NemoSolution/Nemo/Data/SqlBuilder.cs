using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nemo.Attributes;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Configuration.Mapping;

namespace Nemo.Data
{
    internal static class SqlBuilder
    {
        private const string SQL_SELECT_PAGING_FORMAT_MSSQL = "SELECT {1} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {2}) AS __row, {1} FROM {0}{3}) AS t WHERE __row > {4} AND __row <= {5}";
        private const string SQL_SELECT_PAGING_FORMAT_MSSQL_LEGACY = "SELECT * FROM (SELECT TOP {5} * FROM (SELECT TOP {6} {1} FROM {0}{4} ORDER BY {2}) AS t1 ORDER BY {3}) as t2 ORDER BY {2}";
        private const string SQL_SELECT_PAGING_FORMAT = "SELECT {1} FROM {0}{2} LIMIT {3} OFFSET {4}";
        private const string SQL_SELECT_FORMAT = "SELECT {1} FROM {0}";
        private const string SQL_SELECT_COUNT_FORMAT = "SELECT COUNT(*) FROM {0}";
        private const string SQL_WHERE_FORMAT = " WHERE {0}";
        private const string SQL_INNER_JOIN_FORMAT = "{0} INNER JOIN {1} ON {2} {3} {4}";
        private const string SQL_OUTER_JOIN_FORMAT = "{0} LEFT OUTER JOIN {1} ON {2} {3} {4}";
        private const string SQL_INSERT_FORMAT = "INSERT INTO {0} ({1}) VALUES ({2})";
        private const string SQL_UPDATE_FORMAT = "UPDATE {0} SET {1} WHERE {2}";
        private const string SQL_SOFT_DELETE_FORMAT = "UPDATE {0} SET {1} = 1 WHERE {2}";
        private const string SQL_DELETE_FORMAT = "DELETE {0} WHERE {1}";
        private const string SQL_SET_FORMAT = "{2}{0}{3} = {1}";

        public const string DefaultSoftDeleteColumn = "__deleted";
        public const string DefaultTimestampColumn = "__timestamp";

        private static ConcurrentDictionary<Tuple<Type, string, Type>, string> _predicateCache = new ConcurrentDictionary<Tuple<Type, string, Type>, string>();

        internal static string GetTableNameForSql(Type objectType, DialectProvider dialect)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.ExtractInterface(objectType);
            }

            var map = MappingFactory.GetEntityMap(objectType);
            if (map != null)
            {
                tableName = dialect.IdentifierEscapeStartCharacter + map.TableName + dialect.IdentifierEscapeEndCharacter;
                if (!string.IsNullOrEmpty(map.SchemaName))
                {
                    tableName = dialect.IdentifierEscapeStartCharacter + map.SchemaName + dialect.IdentifierEscapeEndCharacter + "." + tableName;
                }
            }

            if (tableName == null)
            {
                var attr = Reflector.GetAttribute<TableAttribute>(objectType);
                if (attr != null)
                {
                    tableName = dialect.IdentifierEscapeStartCharacter + attr.Name + dialect.IdentifierEscapeEndCharacter;
                    if (!string.IsNullOrEmpty(attr.SchemaName))
                    {
                        tableName = dialect.IdentifierEscapeStartCharacter + attr.SchemaName + dialect.IdentifierEscapeEndCharacter + "." + tableName;
                    }
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
        
        internal static string GetSelectStatement<T>(Expression<Func<T, bool>> predicate, int page, int pageSize, DialectProvider dialect)
            where T : class, IDataEntity
        {
            var map = Reflector.GetPropertyMap<T>();
            var selection = map.Values.Where(p => p.IsSelectable && p.IsSimpleType).Select(p => dialect.IdentifierEscapeStartCharacter + p.MappedColumnName + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");
            var tableName = GetTableNameForSql(typeof(T), dialect);
            
            var sql = string.Empty;
            var whereClause = string.Empty;
            if (predicate != null)
            {
                var expression = _predicateCache.GetOrAdd(Tuple.Create(typeof(T), predicate.ToString(), dialect.GetType()), t => ExpressionVisitor.Visit<T>(predicate, dialect));
                whereClause = string.Format(SQL_WHERE_FORMAT, expression);
            }

            if (page > 0 && pageSize > 0)
            {
                if (dialect is SqlServerDialectProvider)
                {
                    if (dialect is SqlServerLegacyDialectProvider)
                    {
                        var primaryKeyAscending = map.Keys.Where(p => map[p].IsPrimaryKey).Select(p => dialect.IdentifierEscapeStartCharacter + map[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter + " ASC").ToDelimitedString(",");
                        var primaryKeyDescending = map.Keys.Where(p => map[p].IsPrimaryKey).Select(p => dialect.IdentifierEscapeStartCharacter + map[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter + " DESC").ToDelimitedString(",");
                        sql = string.Format(SQL_SELECT_PAGING_FORMAT_MSSQL_LEGACY, tableName, selection, primaryKeyAscending, primaryKeyDescending, whereClause, pageSize, page * pageSize);
                    }
                    else
                    {
                        var primaryKey = map.Keys.Where(p => map[p].IsPrimaryKey).Select(p => dialect.IdentifierEscapeStartCharacter + map[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");
                        sql = string.Format(SQL_SELECT_PAGING_FORMAT_MSSQL, tableName, selection, primaryKey, whereClause, (page - 1) * pageSize, page * pageSize);
                    }
                }
                else
                {
                    sql = string.Format(SQL_SELECT_PAGING_FORMAT, tableName, selection, whereClause, pageSize, (page - 1) * pageSize);
                }
            }
            else
            {
                sql = string.Format(SQL_SELECT_FORMAT, tableName, selection) + whereClause;
            }
            return sql;
        }

        internal static string GetSelectCountStatement<T>(Expression<Func<T, bool>> predicate, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(typeof(T), dialect);

            var sql = string.Empty;
            var whereClause = string.Empty;
            if (predicate != null)
            {
                var expression = _predicateCache.GetOrAdd(Tuple.Create(typeof(T), predicate.ToString(), dialect.GetType()), t => ExpressionVisitor.Visit<T>(predicate, dialect));
                whereClause = string.Format(SQL_WHERE_FORMAT, expression);
            }

            sql = string.Format(SQL_SELECT_COUNT_FORMAT, tableName) + whereClause;
            return sql;
        }

        internal static string GetInsertStatement(Type objectType, Param[] parameters, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var columns = parameters.Where(p => !p.IsAutoGenerated).Select(p => dialect.IdentifierEscapeStartCharacter + p.Source + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");
            var paramNames = parameters.Where(p => !p.IsAutoGenerated).Select(p => dialect.UseOrderedParameters ? "?" : dialect.ParameterPrefix + p.Name).ToDelimitedString(",");

            var sql = string.Format(SQL_INSERT_FORMAT, tableName, columns, paramNames);
            return sql;
        }

        internal static string GetUpdateStatement(Type objectType, IList<Param> parameters, IList<Param> primaryKey, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var columns = parameters.Select(p => string.Format(SQL_SET_FORMAT, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            var where = primaryKey.Select(p => string.Format(SQL_SET_FORMAT, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            var sql = string.Format(SQL_UPDATE_FORMAT, tableName, columns, where);
            return sql;
        }

        internal static string GetDeleteStatement(Type objectType, Param[] primaryKey, DialectProvider dialect, string softDeleteColumn = null)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var where = primaryKey.Select(p => string.Format(SQL_SET_FORMAT, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            string sql;
            if (!string.IsNullOrEmpty(softDeleteColumn))
            {
                sql = string.Format(SQL_SOFT_DELETE_FORMAT, tableName, softDeleteColumn, where);
            }
            else
            {
                sql = string.Format(SQL_DELETE_FORMAT, tableName, where);
            }
            return sql;
        }
    }
}
