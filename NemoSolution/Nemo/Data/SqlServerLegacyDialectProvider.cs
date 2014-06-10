using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Data
{
    public class SqlServerLegacyDialectProvider : SqlServerDialectProvider
    {
        public new static SqlServerLegacyDialectProvider Instance = new SqlServerLegacyDialectProvider();

        private SqlServerLegacyDialectProvider()
        {
            AutoIncrementComputation = "SCOPE_IDENTITY()";
            BigIntDefinition = "BIGINT";
            BlobDefition = "VARBINARY(8000)";
            ByteDefinition = "TINYINT";
            ClobDefition = "TEXT";
            DoubleDefinition = "FLOAT";
            SingleDefinition = "REAL";
            GuidDefinition = "UNIQUEIDENTIFIER";
            StringDefinition = "NVARCHAR(4000)";
            AnsiStringDefinition = "VARCHAR(8000)";
            DateDefinition = "DATETIME";
            DateTimeDefinition = "DATETIME";
            TimeDefinition = "DATETIME";
            TemporaryTableCreation = "CREATE TABLE {0} ({1});";
            UseOrderedParameters = false;
            VariableDeclaration = "DECLARE {0}{1} {2};";
            VariableAssignment = "SET {0}{1} = {2};";
            VariablePrefix = "@";
            ParameterPrefix = "@";
            SubstringFunction = "SUBSTRING";
        }

        protected override string PagingTemplate
        {
            get { throw new NotImplementedException(); }
        }
    }
}
