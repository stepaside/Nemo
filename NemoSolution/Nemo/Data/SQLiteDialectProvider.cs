using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class SQLiteDialectProvider : DialectProvider
    {
        public readonly static SQLiteDialectProvider Instance = new SQLiteDialectProvider();

        protected SQLiteDialectProvider()
            : base()
        {
            this.AutoIncrementComputation = "last_insert_rowid()";
            this.BigIntDefinition = "INTEGER";
            this.BlobDefition = "BLOB";
            this.ByteDefinition = "INTEGER";
            this.ClobDefition = "BLOB";
            this.DoubleDefinition = "REAL";
            this.SingleDefinition = "REAL";
            this.GuidDefinition = "TEXT";
            this.StringDefinition = "TEXT";
            this.AnsiStringDefinition = "TEXT";
            this.DateDefinition = "DATETIME";
            this.DateTimeDefinition = "DATETIME";
            this.TimeDefinition = "DATETIME";
            this.TemporaryTableCreation = "CREATE TEMP TABLE IF NOT EXISTS {0} ({1});";
            this.UseOrderedParameters = false;
            this.VariableDeclaration = "CREATE TEMP TABLE IF NOT EXISTS __VARS (name TEXT, value TEXT)";
            this.VariableAssignment = "INSERT INTO __VARS (name, value) VALUES ('{0}{1}', '{2}');";
            this.VariableEvaluation = "(SELECT value FROM __VARS WHERE name = '{0}')";
            this.VariablePrefix = "";
            this.ParameterPrefix = "@";
            this.StringConcatenationOperator = "||";
            this.SubstringFunction = "SUBSTR";
            this.IdentifierEscapeStartCharacter = "\"";
            this.IdentifierEscapeEndCharacter = "\"";
            this.SupportsTemporaryTables = true;
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("INSERT INTO __VARS (name, value) VALUES ('{0}{1}', {2});", VariablePrefix, variableName, AutoIncrementComputation);
        }

        public override string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            var definition = coulmns.Select(d => string.Format("{2}{0}{3} {1}", d.Key, GetColumnType(d.Value), this.IdentifierEscapeStartCharacter, this.IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            return string.Format(TemporaryTableCreation, tableName, definition);
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
