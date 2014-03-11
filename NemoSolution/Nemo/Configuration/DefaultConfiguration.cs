using Nemo.Audit;
using Nemo.Cache;
using Nemo.Cache.Providers;
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
        private int _defaultCacheLifeTime = Config.AppSettings("DefaultCacheLifeTime", 900);
        private bool _cacheCollisionDetection = Config.AppSettings("EnableCacheCollisionDetection", false);
        private bool _logging = Config.AppSettings("EnableLogging", false);
        private string _secretKey = Config.AppSettings("SecretKey", Bytes.ToHex(Bytes.Random(10)));
        private int _staleCacheTimeout = Config.AppSettings("StaleCacheTimeout", 2);
        private int _distributedLockTimeout = Config.AppSettings("DistributedLockTimeout", 60);
        private CacheInvalidationStrategy _cacheInvalidationStrategy = Config.AppSettings<CacheInvalidationStrategy>("CacheInvalidationStrategy", CacheInvalidationStrategy.InvalidateByParameters);

        private ContextLevelCacheType _defaultContextLevelCache = ParseContextLevelCacheConfig();
        private OperationNamingConvention _operationNamingConvention = ParseOperationNamingConventionConfig();
        private FetchMode _defaultFetchMode = ParseFetchModeConfig();
        private MaterializationMode _defaultMaterializationMode = ParseMaterializationModeConfig();
        private ChangeTrackingMode _defaultChangeTrackingMode = ParseChangeTrackingModeConfig();
        private HashAlgorithmName _defaultHashAlgorithm = ParseHashAlgorithmNameConfig();
        private SerializationMode _defaultSerializationMode = SerializationMode.IncludePropertyNames;

        private bool _generateDeleteSql = false;
        private bool _generateInsertSql = false;
        private bool _generateUpdateSql = false;
        private Type _cacheProvider = typeof(MemoryCacheProvider);
        private Type _auditLogProvider = null;

        private DefaultConfiguration() { }

        public int DistributedLockTimeout
        {
            get
            {
                return _distributedLockTimeout;
            }
        }

        public ContextLevelCacheType DefaultContextLevelCache
        {
            get
            {
                return _defaultContextLevelCache;
            }
        }

        public int DefaultCacheLifeTime
        {
            get
            {
                return _defaultCacheLifeTime;
            }
        }

        public bool CacheCollisionDetection
        {
            get
            {
                return _cacheCollisionDetection;
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

        public HashAlgorithmName DefaultHashAlgorithm
        {
            get
            {
                return _defaultHashAlgorithm;
            }
        }

        public string SecretKey
        {
            get
            {
                return _secretKey;
            }
        }

        public int StaleCacheTimeout
        {
            get
            {
                return _staleCacheTimeout;
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

        public Type DefaultCacheProvider
        {
            get
            {
                return _cacheProvider;
            }
        }
        
        public Type AuditLogProvider
        {
            get
            {
                return _auditLogProvider;
            }
        }

        public CacheInvalidationStrategy CacheInvalidationStrategy
        {
            get { return _cacheInvalidationStrategy; }
        }
        
        public static IConfiguration New()
        {
            return new DefaultConfiguration();
        }

        public IConfiguration SetDistributedLockTimeout(int value)
        {
            _distributedLockTimeout = value;
            return this;
        }

        public IConfiguration SetDefaultContextLevelCache(ContextLevelCacheType value)
        {
            _defaultContextLevelCache = value;
            return this;
        }

        public IConfiguration SetDefaultCacheLifeTime(int value)
        {
            _defaultCacheLifeTime = value;
            return this;
        }

        public IConfiguration SetCacheCollisionDetection(bool value)
        {
            _cacheCollisionDetection = value;
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

        public IConfiguration SetDefaultHashAlgorithm(HashAlgorithmName value)
        {
            if (value != HashAlgorithmName.Default)
            {
                _defaultHashAlgorithm = value;
            }
            return this;
        }

        public IConfiguration SetSecretKey(string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                _secretKey = value;
            }
            return this;
        }

        public IConfiguration SetStaleCacheTimeout(int value)
        {
            if (value < 1)
            {
                value = 1;
            }
            _staleCacheTimeout = value;
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

        public IConfiguration SetDefaultCacheProvider(Type value)
        {
            if (value == null || typeof(CacheProvider).IsAssignableFrom(value))
            {
                _cacheProvider = value;
            }
            return this;
        }

        public IConfiguration SetAuditLogProvider(Type value)
        {
            if (value == null || typeof(AuditLogProvider).IsAssignableFrom(value))
            {
                _auditLogProvider = value;
            }
            return this;
        }

        public IConfiguration SetCacheInvalidationStrategy(CacheInvalidationStrategy cacheInvalidationStrategy)
        {
            _cacheInvalidationStrategy = cacheInvalidationStrategy;
            return this;
        }
        
        private static ContextLevelCacheType ParseContextLevelCacheConfig()
        {
            return Config.AppSettings<ContextLevelCacheType>("DefaultContextLevelCache", ContextLevelCacheType.LazyList);
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

        private static HashAlgorithmName ParseHashAlgorithmNameConfig()
        {
            return Config.AppSettings<HashAlgorithmName>("DefaultHashAlgorithmName", HashAlgorithmName.JenkinsHash);
        }
    }
}
