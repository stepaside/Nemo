using Nemo.Logging;
using Nemo.Serialization;
using Nemo.UnitOfWork;

namespace Nemo.Configuration
{
    public interface IConfiguration
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

        IConfiguration SetDefaultCacheRepresentation(CacheRepresentation value);
        IConfiguration SetLogging(bool value);
        IConfiguration SetDefaultMaterializationMode(MaterializationMode value);
        IConfiguration SetOperationPrefix(string value);
        IConfiguration SetDefaultConnectionName(string value);
        IConfiguration SetDefaultChangeTrackingMode(ChangeTrackingMode value);
        IConfiguration SetOperationNamingConvention(OperationNamingConvention value);
        IConfiguration SetDefaultSerializationMode(SerializationMode value);
        IConfiguration SetGenerateDeleteSql(bool value);
        IConfiguration SetGenerateInsertSql(bool value);
        IConfiguration SetGenerateUpdateSql(bool value);
        IConfiguration SetAuditLogProvider(IAuditLogProvider value);
        IConfiguration SetExecutionContext(IExecutionContext value);
        IConfiguration SetHiLoTableName(string value);
        IConfiguration SetLogProvider(ILogProvider value);
        IConfiguration SetAutoTypeCoercion(bool value);
        IConfiguration SetIgnoreInvalidParameters(bool value);
        IConfiguration SetPadListExpansion(bool value);
        IConfiguration Merge(IConfiguration configuration);

#if NETSTANDARD2_0_OR_GREATER
        Microsoft.Extensions.Configuration.IConfiguration SystemConfiguration { get; }
        IConfiguration SetSystemConfiguration(Microsoft.Extensions.Configuration.IConfiguration systemConfiguration);
#endif
    }
}
