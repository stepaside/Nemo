using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Nemo.Attributes;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Linq.Expressions;
using Nemo.Reflection;
using Nemo.Configuration.Mapping;
using ExpressionVisitor = Nemo.Data.PredicateVisitor;

namespace Nemo.Data
{
    internal static class SqlBuilder
    {
        private const string SqlSelectPagingFormatRowNumber = "SELECT {6} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {2}) AS __row, {1} FROM {0}{3}) AS t WHERE __row > {4} AND __row <= {5}";
        private const string SqlSelectPagingFormatMssqlLegacy = "SELECT * FROM (SELECT TOP {5} * FROM (SELECT TOP {6} {1} FROM {0}{4} ORDER BY {2}) AS __t1 ORDER BY {3}) as __t2 ORDER BY {2}";
		private const string SqlSelectPagingFormat = "SELECT {1} FROM {0}{2}{3} LIMIT {4} OFFSET {5}";
        private const string SqlSelectFirstFormatMssql = "SELECT TOP 1 * FROM ({0}) __t";
        private const string SqlSelectFirstFormatOracle = "SELECT * FROM ({0}) __t WHERE rownum = 1";
        private const string SqlSelectFirstFormat = "SELECT * FROM ({0}) __t LIMIT 1";
        private const string SqlSelectFormat = "SELECT {1} FROM {0}";
        private const string SqlSelectCountFormat = "SELECT COUNT(*) FROM {0}";
        private const string SqlWhereFormat = " WHERE {0}";
        private const string SqlInnerJoinClauseFormat = " INNER JOIN {0} ON {1} {2} {3}";
        private const string SqlInnerJoinFormat = "{0} INNER JOIN {1} ON {2} {3} {4}";
        private const string SqlOuterJoinFormat = "{0} LEFT OUTER JOIN {1} ON {2} {3} {4}";
        private const string SqlInsertFormat = "INSERT INTO {0} ({1}) VALUES ({2})";
        private const string SqlUpdateFormat = "UPDATE {0} SET {1} WHERE {2}";
        private const string SqlSoftDeleteFormat = "UPDATE {0} SET {1} = 1 WHERE {2}";
        private const string SqlDeleteFormat = "DELETE FROM {0} WHERE {1}";
        private const string SqlSetFormat = "{2}{0}{3} = {1}";
        
        public const string DefaultSoftDeleteColumn = "__deleted";
        public const string DefaultTimestampColumn = "__timestamp";

        internal static string GetTableNameForSql(Type objectType, DialectProvider dialect)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
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

            if (tableName != null) return tableName;
            
            tableName = objectType.Name;
            if (objectType.IsInterface && tableName[0] == 'I')
            {
                tableName = tableName.Substring(1);
            }

            return tableName;
        }

        internal static string GetOperator(ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                default:
                    return "=";
            }
        }

        internal static string GetSelectStatement<T, T1, T2, T3, T4>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1,
            Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            int page, int pageSize, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            const string aliasRoot = "r";

            var fake = typeof(ObjectFactory.Fake);
            var types = new Dictionary<Type, LambdaExpression>();
            if (typeof(T1) != fake && join1 != null)
            {
                types.Add(typeof(T1), join1);
            }
            if (typeof(T2) != fake && join2 != null)
            {
                types.Add(typeof(T2), join2);
            }
            if (typeof(T3) != fake && join3 != null)
            {
                types.Add(typeof(T3), join3);
            }
            if (typeof(T4) != fake && join4 != null)
            {
                types.Add(typeof(T4), join4);
            }

            var mapRoot = Reflector.GetPropertyNameMap<T>();
            var selection = mapRoot.Values.Where(p => p.IsSelectable && p.IsSimpleType).Select(p => aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + p.MappedColumnName + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");

            var tableName = GetTableNameForSql(typeof(T), dialect) + " " + aliasRoot;

            var index = 1;
            var mapJoinLast = mapRoot;
            var aliasJoinLast = aliasRoot;
            foreach (var type in types)
            {
                var aliasJoin = "t" + index;
                var tableNameJoin = GetTableNameForSql(type.Key, dialect) + " " + aliasJoin;
                var mapJoin = Reflector.GetPropertyNameMap(type.Key);

                var binaryExpression = (BinaryExpression)type.Value.Body;
                var left = (MemberExpression)binaryExpression.Left;
                var right = (MemberExpression)binaryExpression.Right;
                var op = GetOperator(binaryExpression.NodeType);

                tableName += string.Format(SqlInnerJoinClauseFormat, tableNameJoin,
                    aliasJoinLast + "." + dialect.IdentifierEscapeStartCharacter + mapJoinLast[left.Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter, op,
                    aliasJoin + "." + dialect.IdentifierEscapeStartCharacter + mapJoin[right.Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter);
                
                mapJoinLast = mapJoin;
                aliasJoinLast = aliasJoin;
                
                index++;
            }

            if (types.Count > 0)
            {
                selection = mapJoinLast.Values.Where(p => p.IsSelectable && p.IsSimpleType).Select(p => aliasJoinLast + "." + dialect.IdentifierEscapeStartCharacter + p.MappedColumnName + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");
            }

            var sql = string.Empty;
            var whereClause = string.Empty;
            if (predicate != null)
            {
                var evaluated = Evaluator.PartialEval(predicate);
                evaluated = LocalCollectionExpander.Rewrite(evaluated);
                var expression = PredicateVisitor.Visit<T>(evaluated, dialect, aliasRoot);
                whereClause = string.Format(SqlWhereFormat, expression);
            }

            if (page > 0 && pageSize > 0)
            {
                if (dialect is SqlServerLegacyDialectProvider)
                {
                    if (orderBy.Length == 0)
                    {

                        var primaryKeyAscending = mapRoot.Keys.Where(p => mapRoot[p].IsPrimaryKey)
                            .Select(p => aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter + " ASC")
                            .ToDelimitedString(",");
                        var primaryKeyDescending = mapRoot.Keys.Where(p => mapRoot[p].IsPrimaryKey)
                            .Select(p => aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter + " DESC")
                            .ToDelimitedString(",");
                        sql = string.Format(SqlSelectPagingFormatMssqlLegacy, tableName, selection, primaryKeyAscending, primaryKeyDescending, whereClause, pageSize, page * pageSize);
                    }
                    else
                    {
                        var sort = new StringBuilder();
                        var sortReverse = new StringBuilder();
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[((MemberExpression)o.OrderBy.Body).Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                            sortReverse.AppendFormat("{0} {1}, ", column, !o.Reverse ? "DESC" : "ASC");
                        }
                        sort.Length -= 2;
                        sortReverse.Length -= 2;
                        sql = string.Format(SqlSelectPagingFormatMssqlLegacy, tableName, selection, sort, sortReverse, whereClause, pageSize, page * pageSize);
                    }
                }
                else if (dialect is SqlServerDialectProvider || dialect is OracleDialectProvider)
                {
                    var selectionWithoutAlias =
                        mapRoot.Values.Where(p => p.IsSelectable && p.IsSimpleType).Select(p => dialect.IdentifierEscapeStartCharacter + p.MappedColumnName + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");

                    if (orderBy.Length == 0)
                    {
                        var primaryKey =
                            mapRoot.Keys.Where(p => mapRoot[p].IsPrimaryKey)
                                .Select(p => aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[p].MappedColumnName + dialect.IdentifierEscapeEndCharacter)
                                .ToDelimitedString(",");
                        sql = string.Format(SqlSelectPagingFormatRowNumber, tableName, selection, primaryKey, whereClause, (page - 1) * pageSize, page * pageSize, selectionWithoutAlias);
                    }
                    else
                    {
                        var sort = new StringBuilder();
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[((MemberExpression)o.OrderBy.Body).Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                        }
                        sort.Length -= 2;
                        sql = string.Format(SqlSelectPagingFormatRowNumber, tableName, selection, sort, whereClause, (page - 1) * pageSize, page * pageSize, selectionWithoutAlias);
                    }
                }
                else
                {
					var orderByClause = "";
					if (orderBy.Length > 0)
                    {
                        var sort = new StringBuilder(" ORDER BY ");
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[((MemberExpression)o.OrderBy.Body).Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                        }
                        sort.Length -= 2;
						orderByClause = sort.ToString();
                    }
					sql = string.Format(SqlSelectPagingFormat, tableName, selection, whereClause, orderByClause, pageSize, (page - 1) * pageSize);
                }
            }
            else
            {
                sql = string.Format(SqlSelectFormat, tableName, selection) + whereClause;
                if (orderBy.Length > 0)
                {
                    var sort = new StringBuilder(" ORDER BY ");
                    foreach (var o in orderBy)
                    {
                        var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[((MemberExpression)o.OrderBy.Body).Member.Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                        sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                    }
                    sort.Length -= 2;
                    sql += sort;
                }
            }

            if (first)
            {
                if (dialect is SqlServerDialectProvider)
                {
                    sql = string.Format(SqlSelectFirstFormatMssql, sql);
                }
                else if (dialect is OracleDialectProvider)
                {
                    sql = string.Format(SqlSelectFirstFormatOracle, sql);
                }
                else
                {
                    sql = string.Format(SqlSelectFirstFormat, sql);
                }
            }

            return sql;
        }

        internal static string GetSelectStatement<T>(Expression<Func<T, bool>> predicate, int page, int pageSize, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
        {
            return GetSelectStatement<T, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, null, null, null, null, page, pageSize, first, dialect, orderBy);
        }

        internal static string GetSelectStatement<T, T1>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join, int page, int pageSize, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            return GetSelectStatement<T, T1, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join, null, null, null, page, pageSize, first, dialect, orderBy);
        }

        internal static string GetSelectStatement<T, T1, T2>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, int page, int pageSize, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            return GetSelectStatement<T, T1, T2, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join1, join2, null, null, page, pageSize, first, dialect, orderBy);
        }

        internal static string GetSelectStatement<T, T1, T2, T3>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, int page, int pageSize, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
        {
            return GetSelectStatement<T, T1, T2, T3, ObjectFactory.Fake>(predicate, join1, join2, join3, null, page, pageSize, first, dialect, orderBy);
        }

        internal static string GetSelectCountStatement<T>(Expression<Func<T, bool>> predicate, DialectProvider dialect)
        {
            const string aliasRoot = "r";
            var tableName = GetTableNameForSql(typeof(T), dialect) + " " + aliasRoot;

            var sql = string.Empty;
            var whereClause = string.Empty;
            if (predicate != null)
            {
                var evaluated = Evaluator.PartialEval(predicate);
                evaluated = LocalCollectionExpander.Rewrite(evaluated);
                var expression = PredicateVisitor.Visit<T>(evaluated, dialect, aliasRoot);
                whereClause = string.Format(SqlWhereFormat, expression);
            }

            sql = string.Format(SqlSelectCountFormat, tableName) + whereClause;
            return sql;
        }

        internal static string GetInsertStatement(Type objectType, Param[] parameters, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var columns = parameters.Where(p => !p.IsAutoGenerated).Select(p => dialect.IdentifierEscapeStartCharacter + p.Source + dialect.IdentifierEscapeEndCharacter).ToDelimitedString(",");
            var paramNames = parameters.Where(p => !p.IsAutoGenerated).Select(p => dialect.UseOrderedParameters ? "?" : dialect.ParameterPrefix + p.Name).ToDelimitedString(",");
            
            var sql = string.Format(SqlInsertFormat, tableName, columns, paramNames);

            var primaryKey = parameters.FirstOrDefault(p => p.IsAutoGenerated && p.IsPrimaryKey);

            if (primaryKey != null)
            {
                sql += ";" + dialect.ComputeAutoIncrement(primaryKey.Name, () => tableName);
            }

            return sql;
        }

        internal static string GetUpdateStatement(Type objectType, IList<Param> parameters, IList<Param> primaryKey, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var columns = parameters.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            var where = primaryKey.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            var sql = string.Format(SqlUpdateFormat, tableName, columns, where);
            return sql;
        }

        internal static string GetDeleteStatement(Type objectType, IList<Param> primaryKey, DialectProvider dialect, string softDeleteColumn = null)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var where = primaryKey.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            var sql = !string.IsNullOrEmpty(softDeleteColumn) ? string.Format(SqlSoftDeleteFormat, tableName, softDeleteColumn, @where) : string.Format(SqlDeleteFormat, tableName, @where);
            return sql;
        }
    }
}
