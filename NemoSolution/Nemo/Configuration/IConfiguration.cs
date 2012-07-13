using Nemo.Caching;
using Nemo.UnitOfWork;

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
        bool DistributedLocking { get; }

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
        IConfiguration ToggleDistributedLocking(bool value);
    }
}
