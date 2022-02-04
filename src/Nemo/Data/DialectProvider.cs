using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace Nemo.Data
{
    public abstract class DialectProvider
    {
        private readonly Lazy<Regex> _parameterNameMatcher;
        private readonly Lazy<Regex> _parameterNameMatcherWithGroups;
        private readonly Lazy<Regex> _positionalParameterNameMatcher;

        protected DialectProvider() 
        {
            BooleanDefinition = "BIT";
            IntDefinition = "INTEGER";
            SmallIntDefinition = "SMALLINT";

            _parameterNameMatcher = new Lazy<Regex>(() => !string.IsNullOrEmpty(ParameterNameRegexPattern) ? new Regex(ParameterNameRegexPattern, RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled) : null, true);

            _parameterNameMatcherWithGroups = new Lazy<Regex>(() => !string.IsNullOrEmpty(ParameterNameRegexPattern) ? new Regex(ParameterNameRegexPattern.StartsWith("(") ? ParameterNameRegexPattern : $@"(\(\s*?)?({ParameterNameRegexPattern})(\s*?\))?", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled) : null, true);

            _positionalParameterNameMatcher = new Lazy<Regex>(() => new Regex(@"(\(\s*?)?(\?)(\s*?\))?", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled));
        }

        public string BigIntDefinition { get; protected set; }
        public string ByteDefinition { get; protected set; }
        public string BooleanDefinition { get; protected set; }
        public string IntDefinition { get; protected set; }
        public string SmallIntDefinition { get; protected set; }
        public string BlobDefition { get; protected set; }
        public string ClobDefition { get; protected set; }
        public string SingleDefinition { get; protected set; }
        public string DoubleDefinition { get; protected set; }
        public string GuidDefinition { get; protected set; }
        public string StringDefinition { get; protected set; }
        public string AnsiStringDefinition { get; protected set; }
        public string DateDefinition { get; protected set; }
        public string DateTimeDefinition { get; protected set; }
        public string DateTime2Definition { get; protected set; }
        public string DateTimeOffsetDefinition { get; protected set; }
        public string TimeDefinition { get; protected set; }

        public string AutoIncrementComputation { get; protected set; }
        public string TemporaryTableCreation { get; protected set; }
        public string ConditionalTableCreation { get; protected set; }
        public string VariableDeclaration { get; protected set; }
        public string VariableAssignment { get; protected set; }
        public string VariableEvaluation { get; protected set; }
        public string VariablePrefix { get; protected set; }
        public string ParameterPrefix { get; protected set; }
        public bool UseOrderedParameters { get; protected set; }

        public string StringConcatenationOperator { get; protected set; }
        public string StringConcatenationFunction { get; protected set; }
        public string SubstringFunction { get; protected set; }
        public string AutoIncrementSequenceNameSuffix { get; protected set; }

        public bool SupportsTemporaryTables { get; protected set; } 
        public string IdentifierEscapeStartCharacter { get; protected set; }
        public string IdentifierEscapeEndCharacter { get; protected set; }

        public string StoredProcedureParameterListQuery { get; protected set; }
        public string ParameterNameRegexPattern { get; protected set; }

        public bool SupportsArrays { get; protected set; }

        public abstract int MaximumNumberOfParameters { get; }

        public Regex ParameterNameMatcher
        {
            get
            {
                return _parameterNameMatcher.Value;
            }
        }

        public Regex ParameterNameMatcherWithGroups
        {
            get
            {
                return _parameterNameMatcherWithGroups.Value;
            }
        }

        public Regex PositionalParameterMatcher
        {
            get
            {
                return _positionalParameterNameMatcher.Value;
            }
        }

        public virtual string DeclareVariable(string variableName, DbType dbType)
        {
            throw new NotImplementedException();
        }

        public virtual string ComputeAutoIncrement(string variableName, Func<string> tableNameFactory)
        {
            throw new NotImplementedException();
        }

        public virtual string ComputeAutoIncrementSequenceName(string tableName)
        {
            return tableName + "_" + (AutoIncrementSequenceNameSuffix ?? "id_sequence");
        }

        public virtual string CreateTemporaryTable(string tableName, Dictionary<string, DbType> coulmns)
        {
            throw new NotImplementedException();
        }

        public virtual string CreateTableIfNotExists(string tableName, Dictionary<string, Tuple<DbType, int>> coulmns)
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

        public virtual string SplitString(string variableName, string type, string delimiter)
        {
            return null;
        }

        public virtual string GetColumnType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    return AnsiStringDefinition;
                case DbType.AnsiStringFixedLength:
                    return "CHAR";
                case DbType.Binary:
                    return BlobDefition;
                case DbType.Boolean:
                    return BooleanDefinition;
                case DbType.Byte:
                    return ByteDefinition;
                case DbType.Double:
                    return DoubleDefinition;
                case DbType.Guid:
                    return GuidDefinition;
                case DbType.Int16:
                    return SmallIntDefinition;
                case DbType.Int32:
                    return IntDefinition;
                case DbType.Int64:
                    return BigIntDefinition;
                case DbType.Single:
                    return SingleDefinition;
                case DbType.String:
                    return StringDefinition;
                case DbType.StringFixedLength:
                    return "NCHAR";
                case DbType.Date:
                    return DateDefinition;
                case DbType.DateTime:
                    return DateTimeDefinition;
                case DbType.DateTime2:
                    return DateTime2Definition;
                case DbType.Time:
                    return TimeDefinition;
                case DbType.DateTimeOffset:
                    return DateTimeOffsetDefinition;
            }
            return null;
        }

        public virtual bool RequiresSize(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.String:
                case DbType.StringFixedLength:
                    return true;
                default:
                    return false;
            }
        }

        protected abstract string PagingTemplate { get; }

        public virtual string GetPagingSql<T>(string tableName, string selection, string whereClause, int page, int pageSize)
             where T : class
        {
            return null;
        }

        //This is taken from Dapper
        public virtual int GetListExpansionPadding(int count)
        {
            switch (count)
            {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    return 0; // no padding
            }
            if (count < 0) return 0;

            int padFactor;
            if (count <= 150) padFactor = 10;
            else if (count <= 750) padFactor = 50;
            else if (count <= 2000) padFactor = 100; // note: max param count for SQL Server
            else if (count <= 2070) padFactor = 10; // try not to over-pad as we approach that limit
            else if (count <= 2100) return 0; // just don't pad between 2070 and 2100, to minimize the crazy
            else padFactor = 200; // above that, all bets are off!

            // if we have 17, factor = 10; 17 % 10 = 7, we need 3 more
            var intoBlock = count % padFactor;
            return intoBlock == 0 ? 0 : (padFactor - intoBlock);
        }
    }
}
