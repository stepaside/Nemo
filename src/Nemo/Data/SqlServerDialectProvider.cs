using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class SqlServerLatestDialectProvider : SqlServerDialectProvider
    {
        public static new SqlServerLatestDialectProvider Instance = new SqlServerLatestDialectProvider();
    }

    public class SqlServerDialectProvider : DialectProvider
    {
        public static SqlServerDialectProvider Instance = new SqlServerDialectProvider();

        protected SqlServerDialectProvider()
        {
            AutoIncrementComputation = "SCOPE_IDENTITY()";
            BigIntDefinition = "BIGINT";
            BlobDefition = "VARBINARY(MAX)";
            ByteDefinition = "TINYINT";
            ClobDefition = "VARCHAR(MAX)";
            DoubleDefinition = "FLOAT";
            SingleDefinition = "REAL";
            GuidDefinition = "UNIQUEIDENTIFIER";
            StringDefinition = "NVARCHAR(4000)";
            AnsiStringDefinition = "VARCHAR(8000)";
            DateDefinition = "DATE";
            DateTimeDefinition = "DATETIME";
            TimeDefinition = "TIME";
            TemporaryTableCreation = "CREATE TABLE {0} ({1});";
            UseOrderedParameters = false;
            VariableDeclaration = "DECLARE {0}{1} {2};";
            VariableAssignment = "SET {0}{1} = {2};";
            VariablePrefix = "@";
            ParameterPrefix = "@";
            StringConcatenationOperator = "+";
            SubstringFunction = "SUBSTRING";
            IdentifierEscapeStartCharacter = "[";
            IdentifierEscapeEndCharacter = "]";
            SupportsTemporaryTables = true;
            ConditionalTableCreation = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{0}' AND xtype='U') CREATE TABLE [{1}] ({2});";
            ParameterNameRegexPattern = "\\@[\\w#$@]+";
            StoredProcedureParameterListQuery = @"
select 
    specific_name as procedure_name, 
    parameter_name 
from 
    information_schema.parameters 
where 
    specific_name = @name
order by ordinal_position;";
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("SET {0}{1} = {2};", VariablePrefix, variableName, AutoIncrementComputation);
        }

        public override string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            var definition = coulmns.Select(d => string.Format("{2}{0}{3} {1}", d.Key, GetColumnType(d.Value), IdentifierEscapeStartCharacter, IdentifierEscapeEndCharacter)).ToDelimitedString(",");
            return string.Format(TemporaryTableCreation, GetTemporaryTableName(tableName), definition);
        }

        public override string CreateTableIfNotExists(string tableName, Dictionary<string, Tuple<DbType, int>> coulmns)
        {
            var definition =
                coulmns.Select(d => string.Format("{2}{0}{3} {1}{4}", d.Key, GetColumnType(d.Value.Item1), IdentifierEscapeStartCharacter, IdentifierEscapeEndCharacter, RequiresSize(d.Value.Item1) && d.Value.Item2 > 0 ? "(" + d.Value.Item2 + ")" : ""))
                    .ToDelimitedString(",");
            return string.Format(ConditionalTableCreation, tableName.Replace("'", "''"), tableName, definition);
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
