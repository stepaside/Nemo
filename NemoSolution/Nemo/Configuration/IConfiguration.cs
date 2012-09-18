using Nemo.Caching;
using Nemo.UnitOfWork;
using Nemo.Serialization;

namespace Nemo.Configuration
{
    public interface IConfiguration
    {
        bool DistributedLockVerification { get; }
        ContextLevelCacheType ContextLevelCache { get; }
        int CacheLifeTime { get; }
        bool CacheCollisionDetection { get; }
        bool Logging { get; }
        FetchMode DefaultFetchMode { get; }
        MaterializationMode DefaultMaterializationMode { get; }
        string DefaultConnectionName { get; }
        string OperationPrefix { get; }
        ChangeTrackingMode DefaultChangeTrackingMode { get; }
        OperationNamingConvention DefaultOperationNamingConvention { get; }
        HashAlgorithmName DefaultHashAlgorithm { get; }
        string SecretKey { get; }
        CacheContentionMitigationType CacheContentionMitigation { get; }
        int StaleCacheTimeout { get; }
        int DistributedLockTimeout { get; }
        SerializationMode DefaultSerializationMode { get; }
        bool GenerateDeleteSql { get; }
        bool GenerateInsertSql { get; }
        bool GenerateUpdateSql { get; }

        IConfiguration ToggleDistributedLockVerification(bool value);
        IConfiguration SetContextLevelCache(ContextLevelCacheType value);
        IConfiguration SetCacheLifeTime(int value);
        IConfiguration ToggleCacheCollisionDetection(bool value);
        IConfiguration ToggleLogging(bool value);
        IConfiguration SetDefaultFetchMode(FetchMode value);
        IConfiguration SetDefaultMaterializationMode(MaterializationMode value);
        IConfiguration SetOperationPrefix(string value);
        IConfiguration SetDefaultConnectionName(string value);
        IConfiguration SetDefaultChangeTrackingMode(ChangeTrackingMode value);
        IConfiguration SetDefaultOperationNamingConvention(OperationNamingConvention value);
        IConfiguration SetDefaultHashAlgorithm(HashAlgorithmName value);
        IConfiguration SetSecretKey(string value);
        IConfiguration SetCacheContentionMitigation(CacheContentionMitigationType value);
        IConfiguration SetStaleCacheTimeout(int value);
        IConfiguration SetDistributedLockTimeout(int value);
        IConfiguration SetDefaultSerializationMode(SerializationMode value);
        IConfiguration ToggleGenerateDeleteSql(bool value);
        IConfiguration ToggleGenerateInsertSql(bool value);
        IConfiguration ToggleGenerateUpdateSql(bool value);
    }
}
