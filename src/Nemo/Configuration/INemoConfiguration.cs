using Nemo.Logging;
using Nemo.Serialization;
using Nemo.UnitOfWork;

namespace Nemo.Configuration
{
    public interface INemoConfiguration
    {
        CacheRepresentation DefaultCacheRepresentation { get; }
        bool Logging { get; }
        MaterializationMode DefaultMaterializationMode { get; }
        string DefaultConnectionName { get; }
        string OperationPrefix { get; }
        ChangeTrackingMode DefaultChangeTrackingMode { get; }
        OperationNamingConvention OperationNamingConvention { get; }
        SerializationMode DefaultSerializationMode { get; }
        bool GenerateDeleteSql { get; }
        bool GenerateInsertSql { get; }
        bool GenerateUpdateSql { get; }
        IAuditLogProvider AuditLogProvider { get; }
        ILogProvider LogProvider { get; }
        IExecutionContext ExecutionContext { get; }
        string HiLoTableName { get; }
        bool AutoTypeCoercion { get; }
        bool IgnoreInvalidParameters { get; }
        bool PadListExpansion { get; }

        INemoConfiguration SetDefaultCacheRepresentation(CacheRepresentation value);
        INemoConfiguration SetLogging(bool value);
        INemoConfiguration SetDefaultMaterializationMode(MaterializationMode value);
        INemoConfiguration SetOperationPrefix(string value);
        INemoConfiguration SetDefaultConnectionName(string value);
        INemoConfiguration SetDefaultChangeTrackingMode(ChangeTrackingMode value);
        INemoConfiguration SetOperationNamingConvention(OperationNamingConvention value);
        INemoConfiguration SetDefaultSerializationMode(SerializationMode value);
        INemoConfiguration SetGenerateDeleteSql(bool value);
        INemoConfiguration SetGenerateInsertSql(bool value);
        INemoConfiguration SetGenerateUpdateSql(bool value);
        INemoConfiguration SetAuditLogProvider(IAuditLogProvider value);
        INemoConfiguration SetExecutionContext(IExecutionContext value);
        INemoConfiguration SetHiLoTableName(string value);
        INemoConfiguration SetLogProvider(ILogProvider value);
        INemoConfiguration SetAutoTypeCoercion(bool value);
        INemoConfiguration SetIgnoreInvalidParameters(bool value);
        INemoConfiguration SetPadListExpansion(bool value);
        INemoConfiguration Merge(INemoConfiguration configuration);

#if NETSTANDARD2_0_OR_GREATER
        Microsoft.Extensions.Configuration.IConfiguration SystemConfiguration { get; }
        INemoConfiguration SetSystemConfiguration(Microsoft.Extensions.Configuration.IConfiguration systemConfiguration);
#endif
    }
}
