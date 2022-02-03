using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class SqliteDialectProvider : DialectProvider
    {
        public readonly static SqliteDialectProvider Instance = new SqliteDialectProvider();

        protected SqliteDialectProvider()
        {
            AutoIncrementComputation = "last_insert_rowid()";
            BigIntDefinition = "INTEGER";
            BlobDefition = "BLOB";
            ByteDefinition = "INTEGER";
            ClobDefition = "BLOB";
            DoubleDefinition = "REAL";
            SingleDefinition = "REAL";
            GuidDefinition = "TEXT";
            StringDefinition = "TEXT";
            AnsiStringDefinition = "TEXT";
            DateDefinition = "DATETIME";
            DateTimeDefinition = "DATETIME";
            DateTime2Definition = "DATETIME";
            DateTimeOffsetDefinition = "DATETIME";
            TimeDefinition = "DATETIME";
            TemporaryTableCreation = "CREATE TEMP TABLE IF NOT EXISTS {0} ({1});";
            UseOrderedParameters = false;
            VariableDeclaration = "CREATE TEMP TABLE IF NOT EXISTS __VARS (name TEXT, value TEXT)";
            VariableAssignment = "INSERT INTO __VARS (name, value) VALUES ('{0}{1}', '{2}');";
            VariableEvaluation = "(SELECT value FROM __VARS WHERE name = '{0}')";
            VariablePrefix = "";
            ParameterPrefix = "@";
            StringConcatenationOperator = "||";
            SubstringFunction = "SUBSTR";
            IdentifierEscapeStartCharacter = "\"";
            IdentifierEscapeEndCharacter = "\"";
            SupportsTemporaryTables = true;
            ConditionalTableCreation = "CREATE TABLE IF NOT EXISTS {0} ({1})";
            ParameterNameRegexPattern = "\\@[\\w$]+";
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("INSERT INTO __VARS (name, value) VALUES ('{0}{1}', {2});", VariablePrefix, variableName, AutoIncrementComputation);
        }

        public override string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            var definition = coulmns.Select(d => string.Format("{2}{0}{3} {1}", d.Key, GetColumnType(d.Value), IdentifierEscapeStartCharacter, IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            return string.Format(TemporaryTableCreation, tableName, definition);
        }

        public override string CreateTableIfNotExists(string tableName, Dictionary<string, Tuple<DbType, int>> coulmns)
        {
            var definition =
                coulmns.Select(d => string.Format("{2}{0}{3} {1}{4}", d.Key, GetColumnType(d.Value.Item1), IdentifierEscapeStartCharacter, IdentifierEscapeEndCharacter, RequiresSize(d.Value.Item1) && d.Value.Item2 > 0 ? "(" + d.Value.Item2 + ")" : ""))
                    .ToDelimitedString(",");
            return string.Format(ConditionalTableCreation, tableName, definition);
        }

        public override string DeclareVariable(string variableName, DbType dbType)
        {
            return string.Format(VariableDeclaration, VariablePrefix, variableName, GetColumnType(dbType));
        }

        public override string AssignVariable(string variableName, object value)
        {
            var result = "NULL";
            if (value != null && !Convert.IsDBNull(value))
            {
                result = Reflector.IsNumeric(value.GetType()) ? Convert.ToString(value) : "'" + value + "'";
            }
            return string.Format(VariableAssignment, VariablePrefix, variableName, result);
        }

        public override string EvaluateVariable(string variableName)
        {
            return string.Format(VariableEvaluation, variableName);
        }

        public override string GetTemporaryTableName(string tableName)
        {
            if (tableName.StartsWith("TEMP_"))
            {
                return tableName;
            }
            return "TEMP_" + base.GetTemporaryTableName(tableName);
        }

        protected override string PagingTemplate
        {
            get { throw new NotImplementedException(); }
        }
    }
}
