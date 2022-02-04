using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class MySqlDialectProvider : DialectProvider
    {
        public readonly static MySqlDialectProvider Instance = new MySqlDialectProvider();

        protected MySqlDialectProvider()
        {
            AutoIncrementComputation = "LAST_INSERT_ID()";
            BigIntDefinition = "BIGINT";
            BlobDefition = "BLOB";
            ByteDefinition = "TINYINT";
            ClobDefition = "TEXT";
            DoubleDefinition = "DOUBLE";
            SingleDefinition = "FLOAT";
            GuidDefinition = "VARCHAR(36)";
            StringDefinition = "VARCHAR(65535)";
            AnsiStringDefinition = "VARCHAR(65535)";
            DateDefinition = "DATE";
            DateTimeDefinition = "DATETIME";
            DateTime2Definition = "DATETIME";
            DateTimeOffsetDefinition = "DATETIME";
            TimeDefinition = "TIME";
            TemporaryTableCreation = "CREATE TEMPORARY TABLE {0} ({1});";
            UseOrderedParameters = false;
            VariableDeclaration = "DECLARE {0}{1} {2};";
            VariableAssignment = "SET {0}{1} = {2};";
            VariablePrefix = "@";
            ParameterPrefix = "@";
            StringConcatenationFunction = "CONCAT";
            SubstringFunction = "SUBSTRING";
            IdentifierEscapeStartCharacter = "`";
            IdentifierEscapeEndCharacter = "`";
            SupportsTemporaryTables = true;
            ConditionalTableCreation = "CREATE TABLE IF NOT EXISTS {0} ({1})";
            ParameterNameRegexPattern = "\\@([\\w.$]+|\"[^\"]+\"|'[^']+')";
            StoredProcedureParameterListQuery = @"
select 
    r.routine_schema as schema_name,
    r.specific_name as procedure_name,
    p.parameter_name,
    p.data_type,
    case when p.parameter_mode is null and p.data_type is not null
        then 'RETURN'
        else p.parameter_mode 
    end as parameter_mode,
    p.character_maximum_length as char_length,
    p.numeric_precision,
    p.numeric_scale
from 
    information_schema.routines r
    left join information_schema.parameters p
        on p.specific_schema = r.routine_schema 
        and p.specific_name = r.specific_name
where 
    r.routine_schema not in ('sys', 'information_schema', 'mysql', 'performance_schema')
    and r.routine_type = 'PROCEDURE'
    and r.specific_name = @name
order by 
    p.ordinal_position;";
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("SET {0}{1} = {2};", VariablePrefix, variableName, AutoIncrementComputation);
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

        public override int MaximumNumberOfParameters => int.MaxValue;
    }
}
