using System;
using Nemo.Cache;
using Nemo.Serialization;
using Nemo.UnitOfWork;

namespace Nemo.Configuration
{
    public interface IConfiguration
    {
        int DistributedLockTimeout { get; }
        ContextLevelCacheType DefaultContextLevelCache { get; }
        int DefaultCacheLifeTime { get; }
        bool CacheCollisionDetection { get; }
        bool Logging { get; }
        FetchMode DefaultFetchMode { get; }
        MaterializationMode DefaultMaterializationMode { get; }
        string DefaultConnectionName { get; }
        string OperationPrefix { get; }
        ChangeTrackingMode DefaultChangeTrackingMode { get; }
        OperationNamingConvention OperationNamingConvention { get; }
        HashAlgorithmName DefaultHashAlgorithm { get; }
        string SecretKey { get; }
        int StaleCacheTimeout { get; }
        SerializationMode DefaultSerializationMode { get; }
        bool GenerateDeleteSql { get; }
        bool GenerateInsertSql { get; }
        bool GenerateUpdateSql { get; }
        Type DefaultCacheProvider { get; }
        Type AuditLogProvider { get; }
        CacheInvalidationStrategy CacheInvalidationStrategy { get; }

        IConfiguration SetDistributedLockTimeout(int value);
        IConfiguration SetDefaultContextLevelCache(ContextLevelCacheType value);
        IConfiguration SetDefaultCacheLifeTime(int value);
        IConfiguration SetCacheCollisionDetection(bool value);
        IConfiguration SetLogging(bool value);
        IConfiguration SetDefaultFetchMode(FetchMode value);
        IConfiguration SetDefaultMaterializationMode(MaterializationMode value);
        IConfiguration SetOperationPrefix(string value);
        IConfiguration SetDefaultConnectionName(string value);
        IConfiguration SetDefaultChangeTrackingMode(ChangeTrackingMode value);
        IConfiguration SetOperationNamingConvention(OperationNamingConvention value);
        IConfiguration SetDefaultHashAlgorithm(HashAlgorithmName value);
        IConfiguration SetSecretKey(string value);
        IConfiguration SetStaleCacheTimeout(int value);
        IConfiguration SetDefaultSerializationMode(SerializationMode value);
        IConfiguration SetGenerateDeleteSql(bool value);
        IConfiguration SetGenerateInsertSql(bool value);
        IConfiguration SetGenerateUpdateSql(bool value);
        IConfiguration SetDefaultCacheProvider(Type value);
        IConfiguration SetAuditLogProvider(Type value);
        IConfiguration SetCacheInvalidationStrategy(CacheInvalidationStrategy cacheInvalidationStrategy);
    }
}
