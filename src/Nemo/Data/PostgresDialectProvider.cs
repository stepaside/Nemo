using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Nemo.Extensions;
using Nemo.Reflection;

namespace Nemo.Data
{
    public class PostgresDialectProvider : DialectProvider
    {
        public readonly static PostgresDialectProvider Instance = new PostgresDialectProvider();

        protected PostgresDialectProvider()
        {
            AutoIncrementSequenceNameSuffix = "id_sequence";
            IntDefinition = "integer";
            BigIntDefinition = "bigint";
            SmallIntDefinition = "smallint"; 
            BlobDefition = "bytea";
            BooleanDefinition = "boolean";
            ByteDefinition = "smallint";
            ClobDefition = "text";
            DoubleDefinition = "double precision";
            SingleDefinition = "real";
            GuidDefinition = "varchar(36)";
            StringDefinition = "text";
            AnsiStringDefinition = "text";
            DateDefinition = "date";
            DateTimeDefinition = "timestamp";
            DateTime2Definition = "timestamp";
            DateTimeOffsetDefinition = "timestamp with time zone";
            TimeDefinition = "time";
            TemporaryTableCreation = "CREATE TEMPORARY TABLE {0} ({1});";
            UseOrderedParameters = false;
            VariableDeclaration = "DECLARE {0}{1} {2};";
            VariableAssignment = "{0}{1} := {2};";
            VariablePrefix = "";
            ParameterPrefix = "@";
            StringConcatenationOperator = "||";
            SubstringFunction = "substring";
            IdentifierEscapeStartCharacter = "\"";
            IdentifierEscapeEndCharacter = "\"";
            SupportsTemporaryTables = true;
            ConditionalTableCreation = "CREATE TABLE IF NOT EXISTS {0} ({1})";
            ParameterNameRegexPattern = "\\@[\\w$]+";
            StoredProcedureParameterListQuery = @"
select 
    proc.specific_schema as schema_name,
    proc.routine_name as procedure_name,
    args.parameter_name,
    args.parameter_mode,
    args.data_type
from 
    information_schema.routines proc
    left join information_schema.parameters args 
        on proc.specific_schema = args.specific_schema 
        and proc.specific_name = args.specific_name
where 
    proc.routine_schema not in ('pg_catalog', 'information_schema')
    and proc.routine_type = 'PROCEDURE'
	and proc.routine_name = @name
order by
    args.ordinal_position;";
            SupportsArrays = true;
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("{0}{1} := currval('{2}');", VariablePrefix, variableName, ComputeAutoIncrementSequenceName(tableNameFactory()));
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
            if (tableName.StartsWith("temp_"))
            {
                return tableName;
            }
            return "temp_" + base.GetTemporaryTableName(tableName);
        }

        protected override string PagingTemplate
        {
            get { throw new NotImplementedException(); }
        }

        public override int MaximumNumberOfParameters => short.MaxValue;
    }
}
