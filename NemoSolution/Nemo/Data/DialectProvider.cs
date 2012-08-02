using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace Nemo.Data
{
    public abstract class DialectProvider
    {
        protected DialectProvider() { }

        public string BigIntDefinition { get; protected set; }
        public string ByteDefinition { get; protected set; }
        public string BlobDefition { get; protected set; }
        public string ClobDefition { get; protected set; }
        public string SingleDefinition { get; protected set; }
        public string DoubleDefinition { get; protected set; }
        public string GuidDefinition { get; protected set; }
        public string StringDefinition { get; protected set; }
        public string AnsiStringDefinition { get; protected set; }
        public string DateDefinition { get; protected set; }
        public string DateTimeDefinition { get; protected set; }
        public string TimeDefinition { get; protected set; }

        public string AutoIncrementComputation { get; protected set; }
        public string TemporaryTableCreation { get; protected set; }
        public string VariableDeclaration { get; protected set; }
        public string VariableAssignment { get; protected set; }
        public string VariableEvaluation { get; protected set; }
        public string VariablePrefix { get; protected set; }
        public string ParameterPrefix { get; protected set; }
        public bool UseOrderedParameters { get; protected set; }

        public string StringConcatenationOperator { get; protected set; }
        public string StringConcatenationFunction { get; protected set; }
        public string SubstringFunction { get; protected set; }
        public string AutoIncrementSequenceName { get; protected set; }

        public bool SupportsTemporaryTables { get; protected set; } 
        public string IdentifierEscapeStartCharacter { get; protected set; }
        public string IdentifierEscapeEndCharacter { get; protected set; }

        public virtual string DeclareVariable(string variableName, DbType dbType)
        {
            throw new NotImplementedException();
        }

        public virtual string ComputeAutoIncrement(string variableName)
        {
            throw new NotImplementedException();
        }

        public virtual string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            throw new NotImplementedException();
        }

        public virtual string AssignVariable(string variableName, object value)
        {
            throw new NotImplementedException();
        }

        public virtual string EvaluateVariable(string variableName)
        {
            throw new NotImplementedException();
        }

        public virtual string GetTemporaryTableName(string tableName)
        {
            return tableName;
        }

        public virtual string GetColumnType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    return this.AnsiStringDefinition;
                case DbType.AnsiStringFixedLength:
                    return "CHAR";
                case DbType.Binary:
                    return this.BlobDefition;
                case DbType.Boolean:
                    return "BIT";
                case DbType.Byte:
                    return this.ByteDefinition;
                case DbType.Double:
                    return DoubleDefinition;
                case DbType.Guid:
                    return this.GuidDefinition;
                case DbType.Int16:
                    return "SMALLINT";
                case DbType.Int32:
                    return "INTEGER";
                case DbType.Int64:
                    return this.BigIntDefinition;
                case DbType.Single:
                    return this.SingleDefinition;
                case DbType.String:
                    return this.StringDefinition;
                case DbType.StringFixedLength:
                    return "NCHAR";
                case DbType.Date:
                    return this.DateDefinition;
                case DbType.DateTime:
                    return this.DateTimeDefinition;
                case DbType.Time:
                    return this.TimeDefinition;
            }
            return null;
        }

        protected abstract string PagingTemplate { get; }

        public virtual string GetPagingSql<T>(string tableName, string selection, string whereClause, int page, int pageSize)
             where T : class
        {
            return null;
        }
    }
}
