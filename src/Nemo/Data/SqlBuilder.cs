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
using System.ComponentModel;

namespace Nemo.Data
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class SqlBuilder
    {
        private const string SqlSelectPagingFormatRowNumber = "SELECT {6} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {2}) AS __row, {1} FROM {0}{3}) AS t WHERE __row > {4} AND __row <= {5}";
        private const string SqlSelectSkipFormatRowNumber = "SELECT {5} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {2}) AS __row, {1} FROM {0}{3}) AS t WHERE __row > {4}";
        private const string SqlSelectPagingFormatMssqlLegacy = "SELECT * FROM (SELECT TOP {5} * FROM (SELECT TOP {6} {1} FROM {0}{4} ORDER BY {2}) AS __t1 ORDER BY {3}) as __t2 ORDER BY {2}";
        private const string SqlSelectPagingWithOrderByFormat = "SELECT {1} FROM {0}{2} ORDER BY {3} OFFSET {4} ROWS FETCH NEXT {5} ROWS ONLY";
        private const string SqlSelectSkipWithOrderByFormat = "SELECT {1} FROM {0}{2} ORDER BY {3} OFFSET {4} ROWS";
        private const string SqlSelectPagingFormat = "SELECT {1} FROM {0}{2}{3} LIMIT {4} OFFSET {5}";
        private const string SqlSelectSkipFormat = "SELECT {1} FROM {0}{2}{3} OFFSET {4}";
        private const string SqlSelectFirstFormatMssql = "SELECT TOP 1 * FROM ({0}) __t";
        private const string SqlSelectFirstFormatOracle = "SELECT * FROM ({0}) __t WHERE rownum = 1";
        private const string SqlSelectFirstFormat = "SELECT * FROM ({0}) __t LIMIT 1";
        private const string SqlSelectFormat = "SELECT {1} FROM {0}";
        private const string SqlSelectCountFormat = "SELECT COUNT(*) FROM {0}";
        private const string SqlSelectAggregateFormat = "SELECT {0}({1}) FROM {2}";
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

        private static readonly ConcurrentDictionary<(Type Type, DialectProvider Dialect), string> AllTables = new ConcurrentDictionary<(Type, DialectProvider), string>();

        private static readonly ConcurrentDictionary<(Type Type, DialectProvider Dialect, string Alias), string> AllSelections = new ConcurrentDictionary<(Type, DialectProvider, string), string>();

        private static readonly ConcurrentDictionary<(Type Type, DialectProvider Dialect, string Alias), string> AllPrimaryKeys = new ConcurrentDictionary<(Type, DialectProvider, string), string>();

        internal static string GetSelectionForSql(Type objectType, DialectProvider dialect, string alias)
        {
            return AllSelections.GetOrAdd((objectType, dialect, alias), key => BuildColumnList(Reflector.GetPropertyNameMap(key.Type).Values.Where(p => p.IsSelectable && p.IsSimpleType), key.Dialect, key.Alias, null));
        }

        private static string GetPrimaryKeyForSql(Type objectType, DialectProvider dialect, string alias, string suffix = null)
        {
            if (suffix != null)
            {
                return BuildColumnList(Reflector.GetPropertyNameMap(objectType).Values.Where(p => p.IsPrimaryKey), dialect, alias, suffix);
            }

            return AllPrimaryKeys.GetOrAdd((objectType, dialect, alias), key => BuildColumnList(Reflector.GetPropertyNameMap(key.Type).Values.Where(p => p.IsPrimaryKey), key.Dialect, key.Alias, null));
        }

        private static string BuildColumnList(IEnumerable<ReflectedProperty> properties, DialectProvider dialect, string alias, string suffix)
        {
            var columns = new StringBuilder();
            foreach (var property in properties)
            {
                if (columns.Length > 0)
                {
                    columns.Append(',');
                }

                if (!string.IsNullOrEmpty(alias))
                {
                    columns.Append(alias).Append('.');
                }

                columns.Append(dialect.IdentifierEscapeStartCharacter).Append(property.MappedColumnName).Append(dialect.IdentifierEscapeEndCharacter);

                if (suffix != null)
                {
                    columns.Append(suffix);
                }
            }
            return columns.ToString();
        }

        public static string GetTableNameForSql(Type objectType, DialectProvider dialect)
        {
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
            }
            
            if (AllTables.TryGetValue((objectType, dialect), out var tableName))
            {
                return tableName;
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

            if (tableName != null)
            {
                AllTables.TryAdd((objectType, dialect), tableName);
                return tableName;
            }
            
            tableName = objectType.Name;
            if (objectType.IsInterface && tableName[0] == 'I')
            {
                tableName = tableName.Substring(1);
            }
            tableName = dialect.IdentifierEscapeStartCharacter + tableName + dialect.IdentifierEscapeEndCharacter;

            AllTables.TryAdd((objectType, dialect), tableName);

            return tableName;
        }

        private static MemberInfo GetSortMember(Expression body)
        {
            while (body is UnaryExpression unary && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                body = unary.Operand;
            }
            return ((MemberExpression)body).Member;
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

        public static string GetSelectStatement<T, T1, T2, T3, T4>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1,
            Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            int page, int pageSize, int skipCount, bool first,  DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            return GetSelectStatement(predicate, join1, join2, join3, join4, page, pageSize, skipCount, first, dialect, null, orderBy);
        }

        public static string GetSelectStatement<T, T1, T2, T3, T4>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1,
            Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, Expression<Func<T3, T4, bool>> join4,
            int page, int pageSize, int skipCount, bool first,  DialectProvider dialect, IList<Param> parameters, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            const string aliasRoot = "r";

            if (dialect.UseOrderedParameters)
            {
                parameters = null;
            }

            var fake = typeof(ObjectFactory.Fake);
            Dictionary<Type, LambdaExpression> types = null;
            if (typeof(T1) != fake && join1 != null)
            {
                (types = types ?? new Dictionary<Type, LambdaExpression>()).Add(typeof(T1), join1);
            }
            if (typeof(T2) != fake && join2 != null)
            {
                (types = types ?? new Dictionary<Type, LambdaExpression>()).Add(typeof(T2), join2);
            }
            if (typeof(T3) != fake && join3 != null)
            {
                (types = types ?? new Dictionary<Type, LambdaExpression>()).Add(typeof(T3), join3);
            }
            if (typeof(T4) != fake && join4 != null)
            {
                (types = types ?? new Dictionary<Type, LambdaExpression>()).Add(typeof(T4), join4);
            }

            var mapRoot = Reflector.GetPropertyNameMap<T>();
            var selection = GetSelectionForSql(typeof(T), dialect, aliasRoot);

            var tableName = GetTableNameForSql(typeof(T), dialect) + " " + aliasRoot;

            var index = 1;
            var typeJoinLast = typeof(T);
            var mapJoinLast = mapRoot;
            var aliasJoinLast = aliasRoot;
            foreach (var type in types ?? Enumerable.Empty<KeyValuePair<Type, LambdaExpression>>())
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
                
                typeJoinLast = type.Key;
                mapJoinLast = mapJoin;
                aliasJoinLast = aliasJoin;
                
                index++;
            }

            if (types != null)
            {
                selection = GetSelectionForSql(typeJoinLast, dialect, aliasJoinLast);
            }

            var sql = string.Empty;
            var whereClause = string.Empty;
            if (predicate != null)
            {
                var evaluated = Evaluator.PartialEval(predicate);
                evaluated = LocalCollectionExpander.Rewrite(evaluated);
                var expression = PredicateVisitor.Visit<T>(evaluated, dialect, aliasRoot, parameters);
                whereClause = string.Format(SqlWhereFormat, expression);
            }

            var offset = (page > 0 && pageSize > 0 ? (page - 1) * pageSize : 0) + skipCount;
            var limit = pageSize;

            if (offset > 0 || limit > 0)
            {
                if (dialect is SqlServerLegacyDialectProvider)
                {
                    if (orderBy.Length == 0)
                    {
                        var primaryKeyAscending = GetPrimaryKeyForSql(typeof(T), dialect, aliasRoot, " ASC");
                        var primaryKeyDescending = GetPrimaryKeyForSql(typeof(T), dialect, aliasRoot, " DESC");
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingFormatMssqlLegacy, tableName, selection, primaryKeyAscending, primaryKeyDescending, whereClause, limit, offset + limit);
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }
                    else
                    {
                        var sort = new StringBuilder();
                        var sortReverse = new StringBuilder();
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[GetSortMember(o.OrderBy.Body).Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                            sortReverse.AppendFormat("{0} {1}, ", column, !o.Reverse ? "DESC" : "ASC");
                        }
                        sort.Length -= 2;
                        sortReverse.Length -= 2;
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingFormatMssqlLegacy, tableName, selection, sort, sortReverse, whereClause, limit, offset + limit);
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }
                }
                else if (dialect is SqlServerDialectProvider || dialect is OracleDialectProvider)
                {
                    var selectionWithoutAlias = GetSelectionForSql(typeof(T), dialect, null);

                    if (orderBy.Length == 0)
                    {
                        var primaryKey = GetPrimaryKeyForSql(typeof(T), dialect, aliasRoot);
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingFormatRowNumber, tableName, selection, primaryKey, whereClause, offset, offset + limit, selectionWithoutAlias);
                        }
                        else
                        {
                            sql = string.Format(SqlSelectSkipFormatRowNumber, tableName, selection, primaryKey, whereClause, offset, selectionWithoutAlias);
                        }
                    }
                    else
                    {
                        var sort = new StringBuilder();
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[GetSortMember(o.OrderBy.Body).Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                        }
                        sort.Length -= 2;
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingFormatRowNumber, tableName, selection, sort, whereClause, offset, offset + limit, selectionWithoutAlias);
                        }
                        else
                        {
                            sql = string.Format(SqlSelectSkipFormatRowNumber, tableName, selection, sort, whereClause, offset, selectionWithoutAlias);
                        }
                    }
                }
                else if (dialect is SqlServerLatestDialectProvider)
                {
                    if (orderBy.Length == 0)
                    {
                        var primaryKey = GetPrimaryKeyForSql(typeof(T), dialect, aliasRoot);
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingWithOrderByFormat, tableName, selection, whereClause, primaryKey, offset, limit);
                        }
                        else
                        {
                            sql = string.Format(SqlSelectSkipWithOrderByFormat, tableName, selection, whereClause, primaryKey, offset);
                        }
                    }
                    else
                    {
                        var sort = new StringBuilder();
                        foreach (var o in orderBy)
                        {
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[GetSortMember(o.OrderBy.Body).Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                        }
                        sort.Length -= 2;
                        if (limit > 0)
                        {
                            sql = string.Format(SqlSelectPagingWithOrderByFormat, tableName, selection, whereClause, sort, offset, limit);
                        }
                        else
                        {
                            sql = string.Format(SqlSelectSkipWithOrderByFormat, tableName, selection, whereClause, sort, offset);
                        }
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
                            var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[GetSortMember(o.OrderBy.Body).Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
                            sort.AppendFormat("{0} {1}, ", column, !o.Reverse ? "ASC" : "DESC");
                        }
                        sort.Length -= 2;
						orderByClause = sort.ToString();
                    }
                    if (limit > 0)
                    {
                        sql = string.Format(SqlSelectPagingFormat, tableName, selection, whereClause, orderByClause, limit, offset);
                    }
                    else if (dialect is SqliteDialectProvider)
                    {
                        sql = string.Format(SqlSelectPagingFormat, tableName, selection, whereClause, orderByClause, -1, offset);
                    }
                    else if (dialect is MySqlDialectProvider)
                    {
                        sql = string.Format(SqlSelectPagingFormat, tableName, selection, whereClause, orderByClause, ulong.MaxValue, offset);
                    }
                    else
                    {
                        sql = string.Format(SqlSelectSkipFormat, tableName, selection, whereClause, orderByClause, offset);
                    }
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
                        var column = aliasRoot + "." + dialect.IdentifierEscapeStartCharacter + mapRoot[GetSortMember(o.OrderBy.Body).Name].MappedColumnName + dialect.IdentifierEscapeEndCharacter;
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

        public static string GetSelectStatement<T>(Expression<Func<T, bool>> predicate, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
        {
            return GetSelectStatement<T, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, null, null, null, null, page, pageSize, skipCount, first, dialect, orderBy);
        }

        public static string GetSelectStatement<T>(Expression<Func<T, bool>> predicate, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, IList<Param> parameters, params Sorting<T>[] orderBy)
            where T : class
        {
            return GetSelectStatement<T, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, null, null, null, null, page, pageSize, skipCount, first, dialect, parameters, orderBy);
        }

        public static string GetSelectStatement<T, T1>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            return GetSelectStatement<T, T1, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join, null, null, null, page, pageSize, skipCount, first, dialect, orderBy);
        }

        public static string GetSelectStatement<T, T1>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, IList<Param> parameters, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
        {
            return GetSelectStatement<T, T1, ObjectFactory.Fake, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join, null, null, null, page, pageSize, skipCount, first, dialect, parameters, orderBy);
        }

        public static string GetSelectStatement<T, T1, T2>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            return GetSelectStatement<T, T1, T2, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join1, join2, null, null, page, pageSize, skipCount, first, dialect, orderBy);
        }

        public static string GetSelectStatement<T, T1, T2>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, IList<Param> parameters, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
        {
            return GetSelectStatement<T, T1, T2, ObjectFactory.Fake, ObjectFactory.Fake>(predicate, join1, join2, null, null, page, pageSize, skipCount, first, dialect, parameters, orderBy);
        }

        public static string GetSelectStatement<T, T1, T2, T3>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
        {
            return GetSelectStatement<T, T1, T2, T3, ObjectFactory.Fake>(predicate, join1, join2, join3, null, page, pageSize, skipCount, first, dialect, orderBy);
        }

        public static string GetSelectStatement<T, T1, T2, T3>(Expression<Func<T, bool>> predicate, Expression<Func<T, T1, bool>> join1, Expression<Func<T1, T2, bool>> join2, Expression<Func<T2, T3, bool>> join3, int page, int pageSize, int skipCount, bool first, DialectProvider dialect, IList<Param> parameters, params Sorting<T>[] orderBy)
            where T : class
            where T1 : class
            where T2 : class
            where T3 : class
        {
            return GetSelectStatement<T, T1, T2, T3, ObjectFactory.Fake>(predicate, join1, join2, join3, null, page, pageSize, skipCount, first, dialect, parameters, orderBy);
        }

        public static string GetSelectCountStatement<T>(Expression<Func<T, bool>> predicate, DialectProvider dialect)
        {
            return GetSelectCountStatement(predicate, dialect, null);
        }

        public static string GetSelectCountStatement<T>(Expression<Func<T, bool>> predicate, DialectProvider dialect, IList<Param> parameters)
        {
            const string aliasRoot = "r";
            var tableName = GetTableNameForSql(typeof(T), dialect) + " " + aliasRoot;

            if (dialect.UseOrderedParameters)
            {
                parameters = null;
            }

            var whereClause = string.Empty;
            if (predicate != null)
            {
                var evaluated = Evaluator.PartialEval(predicate);
                evaluated = LocalCollectionExpander.Rewrite(evaluated);
                var expression = PredicateVisitor.Visit<T>(evaluated, dialect, aliasRoot, parameters);
                whereClause = string.Format(SqlWhereFormat, expression);
            }

            var sql = string.Format(SqlSelectCountFormat, tableName) + whereClause;
            return sql;
        }

        public static string GetSelectAggregationStatement<T, TColumn>(string aggregateName, Expression<Func<T, TColumn>> projection, Expression<Func<T, bool>> predicate, DialectProvider dialect)
        {
            return GetSelectAggregationStatement(aggregateName, projection, predicate, dialect, null);
        }

        public static string GetSelectAggregationStatement<T, TColumn>(string aggregateName, Expression<Func<T, TColumn>> projection, Expression<Func<T, bool>> predicate, DialectProvider dialect, IList<Param> parameters)
        {
            const string aliasRoot = "r";
            var tableName = GetTableNameForSql(typeof(T), dialect) + " " + aliasRoot;

            if (dialect.UseOrderedParameters)
            {
                parameters = null;
            }

            var whereClause = string.Empty;
            if (predicate != null)
            {
                var evaluated = Evaluator.PartialEval(predicate);
                evaluated = LocalCollectionExpander.Rewrite(evaluated);
                var expression = PredicateVisitor.Visit<T>(evaluated, dialect, aliasRoot, parameters);
                whereClause = string.Format(SqlWhereFormat, expression);
            }

            var memberExpression = (MemberExpression)projection.Body;
            var parentPropertyMap = Reflector.GetPropertyMap(typeof(T));
            parentPropertyMap.TryGetValue((PropertyInfo)memberExpression.Member, out var property);
            var columnName = property != null ? property.MappedColumnName : memberExpression.Member.Name;

            var sql = string.Format(SqlSelectAggregateFormat, aggregateName, dialect.IdentifierEscapeStartCharacter + columnName + dialect.IdentifierEscapeEndCharacter, tableName) + whereClause;
            return sql;
        }

        public static string GetInsertStatement(Type objectType, Param[] parameters, DialectProvider dialect)
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

        public static string GetUpdateStatement(Type objectType, IList<Param> parameters, IList<Param> primaryKey, DialectProvider dialect)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var columns = parameters.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            var where = primaryKey.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            var sql = string.Format(SqlUpdateFormat, tableName, columns, where);
            return sql;
        }

        public static string GetDeleteStatement(Type objectType, IList<Param> primaryKey, DialectProvider dialect, string softDeleteColumn = null)
        {
            var tableName = GetTableNameForSql(objectType, dialect);
            var where = primaryKey.Select(p => string.Format(SqlSetFormat, p.Source, dialect.ParameterPrefix + p.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter)).ToDelimitedString(" AND ");

            var sql = !string.IsNullOrEmpty(softDeleteColumn) ? string.Format(SqlSoftDeleteFormat, tableName, softDeleteColumn, @where) : string.Format(SqlDeleteFormat, tableName, @where);
            return sql;
        }
    }
}
