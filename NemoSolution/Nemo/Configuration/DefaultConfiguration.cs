using Nemo.Caching;
using Nemo.Caching.Providers;
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
        private bool _distributedLockVerification = Config.AppSettings("EnableDistributedLockVerification", false);
        private ContextLevelCacheType _contextLevelCache = ParseContextLevelCacheConfig();
        private int _defaultCacheLifeTime = Config.AppSettings("DefaultCacheLifeTime", 900);
        private bool _cacheCollisionDetection = Config.AppSettings("EnableCacheCollisionDetection", false);
        private bool _logging = Config.AppSettings("EnableLogging", false);
        private FetchMode _defaultFetchMode = FetchMode.Eager;
        private MaterializationMode _defaultMaterializationMode = MaterializationMode.Partial;
        private ChangeTrackingMode _defaultChangeTrackingMode = ChangeTrackingMode.Automatic;
        private OperationNamingConvention _defaultOperationNamingConvention = OperationNamingConvention.PrefixTypeName_Operation;
        private HashAlgorithmName _defaultHashAlgorithm = HashAlgorithmName.JenkinsHash;
        private string _secretKey = Config.AppSettings("SecretKey", Bytes.ToHex(Bytes.Random(10)));
        private CacheContentionMitigationType _cacheContentionMitigation = CacheContentionMitigationType.None;
        private int _staleCacheTimeout = Config.AppSettings("StaleCacheTimeout", 2);
        private int _distributedLockTimeout = Config.AppSettings("DistributedLockTimeout", 2);
        private int _distributedLockRetryCount = Config.AppSettings("DistributedLockRetryCount", 4);
        private double _distributedLockWaitTime = Config.AppSettings("DistributedLockWaitTime", 0.7);
        private SerializationMode _defaultSerializationMode = SerializationMode.IncludePropertyNames;
        private bool _generateDeleteSql = false;
        private bool _generateInsertSql = false;
        private bool _generateUpdateSql = false;
        private Type _cacheProvider = typeof(MemoryCacheProvider);
        
        private DefaultConfiguration() { }

        public bool DistributedLockVerification
        {
            get
            {
                return _distributedLockVerification;
            }
        }

        public ContextLevelCacheType DefaultContextLevelCache
        {
            get
            {
                return _contextLevelCache;
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
                return _defaultOperationNamingConvention;
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

        public CacheContentionMitigationType CacheContentionMitigation
        {
            get
            {
                return _cacheContentionMitigation;
            }
        }

        public int StaleCacheTimeout
        {
            get
            {
                return _staleCacheTimeout;
            }
        }

        public int DistributedLockTimeout
        {
            get
            {
                return _distributedLockTimeout;
            }
        }

        public int DistributedLockRetryCount
        {
            get
            {
                return _distributedLockRetryCount;
            }
        }

        public double DistributedLockWaitTime
        {
            get
            {
                return _distributedLockWaitTime;
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

        public static IConfiguration New()
        {
            return new DefaultConfiguration();
        }

        public IConfiguration SetDistributedLockVerification(bool value)
        {
            _distributedLockVerification = value;
            return this;
        }

        public IConfiguration SetDefaultContextLevelCache(ContextLevelCacheType value)
        {
            _contextLevelCache = value;
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
                _defaultOperationNamingConvention = value;
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

        public IConfiguration SetCacheContentionMitigation(CacheContentionMitigationType value)
        {
            _cacheContentionMitigation = value;
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

        public IConfiguration SetDistributedLockTimeout(int value)
        {
            if (value < 1)
            {
                value = 1;
            }
            _distributedLockTimeout = value;
            return this;
        }

        public IConfiguration SetDistributedLockRetryCount(int value)
        {
            if (value > -1)
            {
                _distributedLockRetryCount = value;
            }
            return this;
        }

        public IConfiguration SetDistributedLockWaitTime(double value)
        {
            if (value > 0)
            {
                _distributedLockWaitTime = value;
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

        public IConfiguration SetDefaultCacheProvider(Type value)
        {
            if (value == null || typeof(CacheProvider).IsAssignableFrom(value))
            {
                _cacheProvider = value;
            }
            return this;
        }
        
        private static ContextLevelCacheType ParseContextLevelCacheConfig()
        {
            var result = ContextLevelCacheType.LazyList;
            var value = Config.AppSettings("ContextLevelCache");
            if (value.NullIfEmpty() == null)
            {
                result = ContextLevelCacheType.LazyList;
            }
            else if (value.ToLower() == "none")
            {
                result = ContextLevelCacheType.None;
            }
            else if (value.ToLower() == "list")
            {
                result = ContextLevelCacheType.List;
            }
            return result;
        }

    }
}
