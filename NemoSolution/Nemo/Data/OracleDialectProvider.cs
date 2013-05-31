using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class OracleDialectProvider : DialectProvider
    {
        public readonly static OracleDialectProvider Instance = new OracleDialectProvider();

        protected OracleDialectProvider()
            : base()
        {
            this.AutoIncrementSequenceNameSuffix = "id_sequence";
            this.BigIntDefinition = "NUMBER(38)";
            this.SmallIntDefinition = "NUMBER(38)";
            this.BooleanDefinition = "NUMBER(1)";
            this.BlobDefition = "LONG RAW";
            this.ByteDefinition = "UNSIGNED INTEGER";
            this.ClobDefition = "CLOB";
            this.DoubleDefinition = "FLOAT(126)";
            this.SingleDefinition = "FLOAT(63)";
            this.GuidDefinition = "RAW(16)";
            this.StringDefinition = "NVARCHAR2(4000)";
            this.AnsiStringDefinition = "VARCHAR2(4000)";
            this.DateDefinition = "DATE";
            this.DateTimeDefinition = "TIMESTAMP";
            this.TimeDefinition = "TIMESTAMP";
            this.TemporaryTableCreation = "CREATE GLOBAL TEMPORARY TABLE {0} ({1}) ON COMMIT DELETE ROWS;";
            this.UseOrderedParameters = false;
            this.VariableDeclaration = "DECLARE {0}{1} {2};";
            this.VariableAssignment = "{0}{1} := {2};";
            this.VariablePrefix = "";
            this.ParameterPrefix = ":";
            this.StringConcatenationOperator = "||";
            this.SubstringFunction = "SUBSTR";
            this.IdentifierEscapeStartCharacter = "\"";
            this.IdentifierEscapeEndCharacter = "\"";
            this.SupportsTemporaryTables = true;
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("{0}{1} := {2}.CURRVAL;", VariablePrefix, variableName, ComputeAutoIncrementSequenceName(tableNameFactory()));
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
            return variableName;
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
