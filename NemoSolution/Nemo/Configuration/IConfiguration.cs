using System;
using Nemo.Caching;
using Nemo.Serialization;
using Nemo.UnitOfWork;

namespace Nemo.Configuration
{
    public interface IConfiguration
    {
        bool DistributedLockVerification { get; }
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
        CacheContentionMitigationType CacheContentionMitigation { get; }
        int StaleCacheTimeout { get; }
        int DistributedLockTimeout { get; }
        int DistributedLockRetryCount { get; }
        double DistributedLockWaitTime { get; }
        BinarySerializationMode DefaultBinarySerializationMode { get; }
        bool GenerateDeleteSql { get; }
        bool GenerateInsertSql { get; }
        bool GenerateUpdateSql { get; }
        Type DefaultCacheProvider { get; }

        IConfiguration SetDistributedLockVerification(bool value);
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
        IConfiguration SetCacheContentionMitigation(CacheContentionMitigationType value);
        IConfiguration SetStaleCacheTimeout(int value);
        IConfiguration SetDistributedLockTimeout(int value);
        IConfiguration SetDistributedLockRetryCount(int value);
        IConfiguration SetDistributedLockWaitTime(double value);
        IConfiguration SetDefaultBinarySerializationMode(BinarySerializationMode value);
        IConfiguration SetGenerateDeleteSql(bool value);
        IConfiguration SetGenerateInsertSql(bool value);
        IConfiguration SetGenerateUpdateSql(bool value);
        IConfiguration SetDefaultCacheProvider(Type value);
    }
}
