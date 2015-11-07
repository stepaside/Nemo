using Nemo.Audit;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;
using System;

namespace Nemo.Configuration
{
    internal sealed class DefaultConfiguration : IConfiguration
    {
        private string _defaultConnectionName;
        private string _operationPrefix;
        private bool? _logging;

        private L1CacheRepresentation? _defaultL1CacheRepresentation;
        private OperationNamingConvention? _operationNamingConvention;
        private FetchMode? _defaultFetchMode;
        private MaterializationMode? _defaultMaterializationMode;
        private ChangeTrackingMode? _defaultChangeTrackingMode;
        private SerializationMode? _defaultSerializationMode;

        private bool _generateDeleteSql;
        private bool _generateInsertSql;
        private bool _generateUpdateSql;
        private IAuditLogProvider _auditLogProvider;
        private IExecutionContext _executionContext = DefaultExecutionContext.Current;
        private string _hiLoTableName;

        public L1CacheRepresentation DefaultL1CacheRepresentation
        {
            get
            {
                return _defaultL1CacheRepresentation.HasValue ? _defaultL1CacheRepresentation.Value : ParseExecutionContextCacheConfig();
            }
        }

        public bool Logging
        {
            get { return _logging.HasValue ? _logging.Value : Config.AppSettings("EnableLogging", false); }
        }

        public FetchMode DefaultFetchMode
        {
            get { return _defaultFetchMode.HasValue ? _defaultFetchMode.Value : ParseFetchModeConfig(); }
        }

        public MaterializationMode DefaultMaterializationMode
        {
            get { return _defaultMaterializationMode.HasValue ? _defaultMaterializationMode.Value : ParseMaterializationModeConfig(); }
        }

        public string DefaultConnectionName
        {
            get { return _defaultConnectionName ?? Config.AppSettings("DefaultConnectionName", "DbConnection"); }
        }

        public string OperationPrefix
        {
            get { return _operationPrefix ?? Config.AppSettings("OperationPrefix", string.Empty); }
        }

        public ChangeTrackingMode DefaultChangeTrackingMode
        {
            get { return _defaultChangeTrackingMode.HasValue ? _defaultChangeTrackingMode.Value : ParseChangeTrackingModeConfig(); }
        }

        public OperationNamingConvention OperationNamingConvention
        {
            get { return _operationNamingConvention.HasValue ? _operationNamingConvention.Value : ParseOperationNamingConventionConfig(); }
        }

        public SerializationMode DefaultSerializationMode
        {
            get { return _defaultSerializationMode.HasValue ? _defaultSerializationMode.Value : SerializationMode.IncludePropertyNames; }
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

        public string HiLoTableName
        {
            get { return _hiLoTableName ?? Config.AppSettings("HiLoTableName", "HiLoIdGenerator"); }
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

        public IConfiguration SetHiLoTableName(string value)
        {
            _hiLoTableName = value;
            return this;
        }

        private static L1CacheRepresentation ParseExecutionContextCacheConfig()
        {
            return Config.AppSettings("DefaultExecutionContextCacheType", L1CacheRepresentation.LazyList);
        }

        private static OperationNamingConvention ParseOperationNamingConventionConfig()
        {
            return Config.AppSettings("OperationNamingConvention", OperationNamingConvention.PrefixTypeName_Operation);
        }

        private static FetchMode ParseFetchModeConfig()
        {
            return Config.AppSettings("DefaultFetchMode", FetchMode.Eager);
        }

        private static MaterializationMode ParseMaterializationModeConfig()
        {
            return Config.AppSettings("DefaultMaterializationMode", MaterializationMode.Partial);
        }

        private static ChangeTrackingMode ParseChangeTrackingModeConfig()
        {
            return Config.AppSettings("DefaultChangeTrackingMode", ChangeTrackingMode.Automatic);
        }

        public IConfiguration Merge(IConfiguration configuration)
        {
            var mergedConfig = new DefaultConfiguration();

            mergedConfig.SetAuditLogProvider(_auditLogProvider ?? configuration.AuditLogProvider);

            mergedConfig.SetDefaultChangeTrackingMode(_defaultChangeTrackingMode.HasValue ? _defaultChangeTrackingMode.Value : configuration.DefaultChangeTrackingMode);

            mergedConfig.SetDefaultConnectionName(_defaultConnectionName ?? configuration.DefaultConnectionName);

            mergedConfig.SetDefaultFetchMode(_defaultFetchMode.HasValue ? _defaultFetchMode.Value : configuration.DefaultFetchMode);

            mergedConfig.SetDefaultL1CacheRepresentation(_defaultL1CacheRepresentation.HasValue ? _defaultL1CacheRepresentation.Value : configuration.DefaultL1CacheRepresentation);

            mergedConfig.SetDefaultMaterializationMode(_defaultMaterializationMode.HasValue ? _defaultMaterializationMode.Value : configuration.DefaultMaterializationMode);

            mergedConfig.SetDefaultSerializationMode(_defaultSerializationMode.HasValue ? _defaultSerializationMode.Value : configuration.DefaultSerializationMode);

            mergedConfig.SetExecutionContext(_executionContext ?? configuration.ExecutionContext);

            mergedConfig.SetGenerateDeleteSql(GenerateDeleteSql || configuration.GenerateDeleteSql);

            mergedConfig.SetGenerateInsertSql(GenerateInsertSql || configuration.GenerateInsertSql);

            mergedConfig.SetGenerateUpdateSql(GenerateUpdateSql || configuration.GenerateUpdateSql);

            mergedConfig.SetLogging(Logging || configuration.Logging);

            mergedConfig.SetOperationNamingConvention(_operationNamingConvention.HasValue ? _operationNamingConvention.Value : configuration.OperationNamingConvention);

            mergedConfig.SetOperationPrefix(_operationPrefix ?? configuration.OperationPrefix);

            mergedConfig.SetHiLoTableName(_hiLoTableName ?? configuration.HiLoTableName);

            return mergedConfig;
        }
    }
}
