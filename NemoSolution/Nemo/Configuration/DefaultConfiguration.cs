using Nemo.Audit;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;
using System;

namespace Nemo.Configuration
{
    public sealed class DefaultConfiguration : IConfiguration
    {
        private string _defaultConnectionName = Config.AppSettings("DefaultConnectionName", "DbConnection");
        private string _operationPrefix = Config.AppSettings("OperationPrefix", string.Empty);
        private bool _logging = Config.AppSettings("EnableLogging", false);

        private L1CacheRepresentation _defaultL1CacheRepresentation = ParseExecutionContextCacheConfig();
        private OperationNamingConvention _operationNamingConvention = ParseOperationNamingConventionConfig();
        private FetchMode _defaultFetchMode = ParseFetchModeConfig();
        private MaterializationMode _defaultMaterializationMode = ParseMaterializationModeConfig();
        private ChangeTrackingMode _defaultChangeTrackingMode = ParseChangeTrackingModeConfig();
        private SerializationMode _defaultSerializationMode = SerializationMode.IncludePropertyNames;

        private bool _generateDeleteSql = false;
        private bool _generateInsertSql = false;
        private bool _generateUpdateSql = false;
        private IAuditLogProvider _auditLogProvider = null;
        private IExecutionContext _executionContext = DefaultExecutionContext.Current;

        private DefaultConfiguration() { }
        
        public L1CacheRepresentation DefaultL1CacheRepresentation
        {
            get
            {
                return _defaultL1CacheRepresentation;
            }
        }

        public bool Logging
        {
            get
            {
                return _logging;
            }
        }

        public FetchMode DefaultFetchMode
        {
            get
            {
                return _defaultFetchMode;
            }
        }

        public MaterializationMode DefaultMaterializationMode
        {
            get
            {
                return _defaultMaterializationMode;
            }
        }

        public string DefaultConnectionName
        {
            get
            {
                return _defaultConnectionName;
            }
        }

        public string OperationPrefix
        {
            get
            {
                return _operationPrefix;
            }
        }

        public ChangeTrackingMode DefaultChangeTrackingMode
        {
            get
            {
                return _defaultChangeTrackingMode;
            }
        }

        public OperationNamingConvention OperationNamingConvention
        {
            get
            {
                return _operationNamingConvention;
            }
        }

        public SerializationMode DefaultSerializationMode
        {
            get
            {
                return _defaultSerializationMode;
            }
        }

        public bool GenerateDeleteSql
        {
            get
            {
                return _generateDeleteSql;
            }
        }

        public bool GenerateInsertSql
        {
            get
            {
                return _generateInsertSql;
            }
        }

        public bool GenerateUpdateSql
        {
            get
            {
                return _generateUpdateSql;
            }
        }

        public IAuditLogProvider AuditLogProvider
        {
            get
            {
                return _auditLogProvider;
            }
        }

        public IExecutionContext ExecutionContext
        {
            get
            {
                return _executionContext;
            }
        }
       
        public static IConfiguration New()
        {
            return new DefaultConfiguration();
        }

        public IConfiguration SetDefaultL1CacheRepresentation(L1CacheRepresentation value)
        {
            _defaultL1CacheRepresentation = value;
            return this;
        }
        
        public IConfiguration SetLogging(bool value)
        {
            _logging = value;
            return this;
        }

        public IConfiguration SetDefaultFetchMode(FetchMode value)
        {
            if (value != FetchMode.Default)
            {
                _defaultFetchMode = value;
            }
            return this;
        }

        public IConfiguration SetDefaultMaterializationMode(MaterializationMode value)
        {
            if (value != MaterializationMode.Default)
            {
                _defaultMaterializationMode = value;
            }
            return this;
        }

        public IConfiguration SetOperationPrefix(string value)
        {
            _operationPrefix = value;
            return this;
        }

        public IConfiguration SetDefaultConnectionName(string value)
        {
            _defaultConnectionName = value;
            return this;
        }

        public IConfiguration SetDefaultChangeTrackingMode(ChangeTrackingMode value)
        {
            if (value != ChangeTrackingMode.Default)
            {
                _defaultChangeTrackingMode = value;
            }
            return this;
        }

        public IConfiguration SetOperationNamingConvention(OperationNamingConvention value)
        {
            if (value != OperationNamingConvention.Default)
            {
                _operationNamingConvention = value;
            }
            return this;
        }
                
        public IConfiguration SetDefaultSerializationMode(SerializationMode value)
        {
            _defaultSerializationMode = value;
            return this;
        }

        public IConfiguration SetGenerateDeleteSql(bool value)
        {
            _generateDeleteSql = value;
            return this;
        }

        public IConfiguration SetGenerateInsertSql(bool value)
        {
            _generateInsertSql = value;
            return this;
        }

        public IConfiguration SetGenerateUpdateSql(bool value)
        {
            _generateUpdateSql = value;
            return this;
        }

        public IConfiguration SetAuditLogProvider(IAuditLogProvider value)
        {
            _auditLogProvider = value;
            return this;
        }

        public IConfiguration SetExecutionContext(IExecutionContext value)
        {
            _executionContext = value;
            return this;
        }

        private static L1CacheRepresentation ParseExecutionContextCacheConfig()
        {
            return Config.AppSettings<L1CacheRepresentation>("DefaultExecutionContextCacheType", L1CacheRepresentation.LazyList);
        }

        private static OperationNamingConvention ParseOperationNamingConventionConfig()
        {
            return Config.AppSettings<OperationNamingConvention>("OperationNamingConvention", OperationNamingConvention.PrefixTypeName_Operation);
        }

        private static FetchMode ParseFetchModeConfig()
        {
            return Config.AppSettings<FetchMode>("DefaultFetchMode", FetchMode.Eager);
        }

        private static MaterializationMode ParseMaterializationModeConfig()
        {
            return Config.AppSettings<MaterializationMode>("DefaultMaterializationMode", MaterializationMode.Partial);
        }

        private static ChangeTrackingMode ParseChangeTrackingModeConfig()
        {
            return Config.AppSettings<ChangeTrackingMode>("DefaultChangeTrackingMode", ChangeTrackingMode.Automatic);
        }
    }
}
