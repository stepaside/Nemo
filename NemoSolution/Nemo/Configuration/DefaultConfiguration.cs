using Nemo.Caching;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;

namespace Nemo.Configuration
{
    public sealed class DefaultConfiguration : IConfiguration
    {
        private string _defaultConnectionName = Config.AppSettings("DefaultConnectionName", "DbConnection");
        private string _operationPrefix = Config.AppSettings("OperationPrefix", string.Empty);
        private bool _distributedLockVerification = Config.AppSettings("EnableDistributedLockVerification", false);
        private ContextLevelCacheType _contextLevelCache = ParseContextLevelCacheConfig();
        private int _cacheLifeTime = Config.AppSettings("CacheLifeTime", 900);
        private bool _cacheCollisionDetection = Config.AppSettings("EnableCacheCollisionDetection", false);
        private bool _logging = Config.AppSettings("EnableLogging", false);
        private FetchMode _defaultFetchMode = FetchMode.Eager;
        private MaterializationMode _defaultMaterializationMode = MaterializationMode.Partial;
        private ChangeTrackingMode _defaultChangeTrackingMode = ChangeTrackingMode.Automatic;
        private OperationNamingConvention _defaultOperationNamingConvention = OperationNamingConvention.PrefixTypeName_Operation;
        private HashAlgorithmName _defaultHashAlgorithm = HashAlgorithmName.JenkinsHash;
        private string _secretKey = Config.AppSettings("SecretKey", Bytes.ToHex(Bytes.Random(10)));
        private CacheContentionMitigationType _cacheContentionMitigation = CacheContentionMitigationType.None;
        private int _staleCacheTimeout = 2;
        private int _distributedLockTimeout = 2;
        private SerializationMode _defaultSerializationMode = SerializationMode.IncludePropertyNames;
        private bool _generateDeleteSql = false;
        private bool _generateInsertSql = false;
        private bool _generateUpdateSql = false;

        private DefaultConfiguration() { }

        public bool DistributedLockVerification
        {
            get
            {
                return _distributedLockVerification;
            }
        }

        public ContextLevelCacheType ContextLevelCache
        {
            get
            {
                return _contextLevelCache;
            }
        }

        public int CacheLifeTime
        {
            get
            {
                return _cacheLifeTime;
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

        public OperationNamingConvention DefaultOperationNamingConvention
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

        public static IConfiguration New()
        {
            return new DefaultConfiguration();
        }

        public IConfiguration ToggleDistributedLockVerification(bool value)
        {
            _distributedLockVerification = value;
            return this;
        }

        public IConfiguration SetContextLevelCache(ContextLevelCacheType value)
        {
            _contextLevelCache = value;
            return this;
        }

        public IConfiguration SetCacheLifeTime(int value)
        {
            _cacheLifeTime = value;
            return this;
        }

        public IConfiguration ToggleCacheCollisionDetection(bool value)
        {
            _cacheCollisionDetection = value;
            return this;
        }

        public IConfiguration ToggleLogging(bool value)
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

        public IConfiguration SetDefaultOperationNamingConvention(OperationNamingConvention value)
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

        public IConfiguration SetDefaultSerializationMode(SerializationMode value)
        {
            _defaultSerializationMode = value;
            return this;
        }

        public IConfiguration ToggleGenerateDeleteSql(bool value)
        {
            _generateDeleteSql = value;
            return this;
        }

        public IConfiguration ToggleGenerateInsertSql(bool value)
        {
            _generateInsertSql = value;
            return this;
        }

        public IConfiguration ToggleGenerateUpdateSql(bool value)
        {
            _generateUpdateSql = value;
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
