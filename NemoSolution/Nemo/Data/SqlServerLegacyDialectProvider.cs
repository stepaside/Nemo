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
            : base()
        {
            this.AutoIncrementComputation = "SCOPE_IDENTITY()";
            this.BigIntDefinition = "BIGINT";
            this.BlobDefition = "VARBINARY(8000)";
            this.ByteDefinition = "TINYINT";
            this.ClobDefition = "TEXT";
            this.DoubleDefinition = "FLOAT";
            this.SingleDefinition = "REAL";
            this.GuidDefinition = "UNIQUEIDENTIFIER";
            this.StringDefinition = "NVARCHAR(4000)";
            this.AnsiStringDefinition = "VARCHAR(8000)";
            this.DateDefinition = "DATETIME";
            this.DateTimeDefinition = "DATETIME";
            this.TimeDefinition = "DATETIME";
            this.TemporaryTableCreation = "CREATE TABLE {0} ({1});";
            this.UseOrderedParameters = false;
            this.VariableDeclaration = "DECLARE {0}{1} {2};";
            this.VariableAssignment = "SET {0}{1} = {2};";
            this.VariablePrefix = "@";
            this.ParameterPrefix = "@";
            this.SubstringFunction = "SUBSTRING";
        }

        protected override string PagingTemplate
        {
            get { throw new NotImplementedException(); }
        }
    }
}
