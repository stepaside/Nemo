using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class SqlServerDialectProvider : DialectProvider
    {
        public static SqlServerDialectProvider Instance = new SqlServerDialectProvider();

        protected SqlServerDialectProvider()
            : base()
        {
            this.AutoIncrementComputation = "SCOPE_IDENTITY()";
            this.BigIntDefinition = "BIGINT";
            this.BlobDefition = "VARBINARY(MAX)";
            this.ByteDefinition = "TINYINT";
            this.ClobDefition = "VARCHAR(MAX)";
            this.DoubleDefinition = "FLOAT";
            this.SingleDefinition = "REAL";
            this.GuidDefinition = "UNIQUEIDENTIFIER";
            this.StringDefinition = "NVARCHAR(4000)";
            this.AnsiStringDefinition = "VARCHAR(8000)";
            this.DateDefinition = "DATE";
            this.DateTimeDefinition = "DATETIME";
            this.TimeDefinition = "TIME";
            this.TemporaryTableCreation = "CREATE TABLE {0} ({1});";
            this.UseOrderedParameters = false;
            this.VariableDeclaration = "DECLARE {0}{1} {2};";
            this.VariableAssignment = "SET {0}{1} = {2};";
            this.VariablePrefix = "@";
            this.ParameterPrefix = "@";
            this.StringConcatenationOperator = "+";
            this.SubstringFunction = "SUBSTRING";
            this.IdentifierEscapeStartCharacter = "[";
            this.IdentifierEscapeEndCharacter = "]";
            this.SupportsTemporaryTables = true;
        }

        public override string ComputeAutoIncrement(string variableName)
        {
            return string.Format("SET {0}{1} = {2};", VariablePrefix, variableName, AutoIncrementComputation);
        }

        public override string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            var definition = coulmns.Select(d => string.Format("{2}{0}{3} {1}", d.Key, GetColumnType(d.Value), this.IdentifierEscapeStartCharacter, this.IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            return string.Format(TemporaryTableCreation, GetTemporaryTableName(tableName), definition);
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
            if (tableName.StartsWith("#"))
            {
                return tableName;
            }
            return "#" + base.GetTemporaryTableName(tableName);
        }

        protected override string PagingTemplate
        {
            get { throw new NotImplementedException(); }
        }
    }
}
