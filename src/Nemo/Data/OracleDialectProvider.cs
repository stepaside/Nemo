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
        {
            AutoIncrementSequenceNameSuffix = "id_sequence";
            BigIntDefinition = "NUMBER(38)";
            SmallIntDefinition = "NUMBER(38)";
            BooleanDefinition = "NUMBER(1)";
            BlobDefition = "LONG RAW";
            ByteDefinition = "UNSIGNED INTEGER";
            ClobDefition = "CLOB";
            DoubleDefinition = "FLOAT(126)";
            SingleDefinition = "FLOAT(63)";
            GuidDefinition = "RAW(16)";
            StringDefinition = "NVARCHAR2(4000)";
            AnsiStringDefinition = "VARCHAR2(4000)";
            DateDefinition = "DATE";
            DateTimeDefinition = "TIMESTAMP";
            TimeDefinition = "TIMESTAMP";
            TemporaryTableCreation = "CREATE GLOBAL TEMPORARY TABLE {0} ({1}) ON COMMIT DELETE ROWS;";
            UseOrderedParameters = false;
            VariableDeclaration = "DECLARE {0}{1} {2};";
            VariableAssignment = "{0}{1} := {2};";
            VariablePrefix = "";
            ParameterPrefix = ":";
            StringConcatenationOperator = "||";
            SubstringFunction = "SUBSTR";
            IdentifierEscapeStartCharacter = "\"";
            IdentifierEscapeEndCharacter = "\"";
            SupportsTemporaryTables = true;
            ConditionalTableCreation = "CREATE TABLE IF NOT EXISTS {0} ({1})";
            StoredProcedureParameterListQuery = @"
select 
    proc.owner as schema_name,
    proc.object_name as procedure_name,
    args.argument_name as parameter_name,
    args.in_out,
    args.data_type,
    args.data_length,
    args.data_precision,
    args.data_scale,
    args.defaulted,
    args.default_value
from 
    sys.all_procedures proc
    left join sys.all_arguments args
        on proc.object_id = args.object_id
where 
    proc.owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS',
        'MDSYS', 'MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS', 'ORDSYS',
        'OUTLN', 'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM', 'TSMSYS',
        'WK_TEST', 'WKSYS', 'WKPROXY','WMSYS','XDB','APEX_040000', 
        'APEX_PUBLIC_USER','DIP', 'FLOWS_30000','FLOWS_FILES','MDDATA',
        'ORACLE_OCM', 'XS$NULL', 'SPATIAL_CSW_ADMIN_USR', 'LBACSYS',
        'SPATIAL_WFS_ADMIN_USR', 'PUBLIC', 'APEX_040200')
    and object_type = 'PROCEDURE'
order by args.position;";
        }

        public override string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            return string.Format("{0}{1} := {2}.CURRVAL;", VariablePrefix, variableName, ComputeAutoIncrementSequenceName(tableNameFactory()));
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
    }
}
