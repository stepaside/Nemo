using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;
using System;
using Nemo.Logging;
using Microsoft.Extensions.Configuration;

namespace Nemo.Configuration
{
    internal sealed class DefaultConfiguration : IConfiguration
    {
        private string _defaultConnectionName;
        private string _operationPrefix;
        private bool? _logging;

        private CacheRepresentation? _defaultL1CacheRepresentation;
        private OperationNamingConvention? _operationNamingConvention;
        private MaterializationMode? _defaultMaterializationMode;
        private ChangeTrackingMode? _defaultChangeTrackingMode;
        private SerializationMode? _defaultSerializationMode;

        private string _hiLoTableName;

        public CacheRepresentation DefaultCacheRepresentation => _defaultL1CacheRepresentation ?? CacheRepresentation.LazyList;

        public bool Logging => _logging ?? false;

        public MaterializationMode DefaultMaterializationMode => _defaultMaterializationMode ?? MaterializationMode.Partial;

        public string DefaultConnectionName => _defaultConnectionName ?? "DbConnection";

        public string OperationPrefix => _operationPrefix ?? string.Empty;

        public ChangeTrackingMode DefaultChangeTrackingMode => _defaultChangeTrackingMode ?? ChangeTrackingMode.Automatic;

        public OperationNamingConvention OperationNamingConvention => _operationNamingConvention ?? OperationNamingConvention.PrefixTypeName_Operation;

        public SerializationMode DefaultSerializationMode => _defaultSerializationMode ?? SerializationMode.IncludePropertyNames;

        public bool GenerateDeleteSql { get; private set; }

        public bool GenerateInsertSql { get; private set; }

        public bool GenerateUpdateSql { get; private set; }

        public IAuditLogProvider AuditLogProvider { get; private set; }

        public IExecutionContext ExecutionContext { get; private set; } = DefaultExecutionContext.Current;

        public string HiLoTableName => _hiLoTableName ?? "HiLoIdGenerator";

        public ILogProvider LogProvider { get; private set; }

        public bool AutoTypeCoercion { get; private set; }

        public bool IgnoreInvalidParameters { get; private set; }

        public Microsoft.Extensions.Configuration.IConfiguration SystemConfiguration { get; private set; }

        public bool PadListExpansion { get; private set; }

        public IConfiguration SetDefaultCacheRepresentation(CacheRepresentation value)
        {
            _defaultL1CacheRepresentation = value;
            return this;
        }

        public IConfiguration SetLogging(bool value)
        {
            _logging = value;
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
            GenerateDeleteSql = value;
            return this;
        }

        public IConfiguration SetGenerateInsertSql(bool value)
        {
            GenerateInsertSql = value;
            return this;
        }

        public IConfiguration SetGenerateUpdateSql(bool value)
        {
            GenerateUpdateSql = value;
            return this;
        }

        public IConfiguration SetAuditLogProvider(IAuditLogProvider value)
        {
            AuditLogProvider = value;
            return this;
        }

        public IConfiguration SetExecutionContext(IExecutionContext value)
        {
            ExecutionContext = value;
            return this;
        }

        public IConfiguration SetHiLoTableName(string value)
        {
            _hiLoTableName = value;
            return this;
        }

        public IConfiguration SetLogProvider(ILogProvider value)
        {
            LogProvider = value;
            return this;
        }

        public IConfiguration SetAutoTypeCoercion(bool value)
        {
            AutoTypeCoercion = value;
            return this;
        }

        public IConfiguration SetIgnoreInvalidParameters(bool value)
        {
            IgnoreInvalidParameters = value;
            return this;
        }

        public IConfiguration SetPadListExpansion(bool value)
        {
            PadListExpansion = value;
            return this;
        }

        public IConfiguration SetSystemConfiguration(Microsoft.Extensions.Configuration.IConfiguration systemConfiguration)
        {
            SystemConfiguration = systemConfiguration;
            return this;
        }

        public IConfiguration Merge(IConfiguration configuration)
        {
            var mergedConfig = new DefaultConfiguration();

            mergedConfig.SetAuditLogProvider(AuditLogProvider ?? configuration.AuditLogProvider);

            mergedConfig.SetDefaultChangeTrackingMode(_defaultChangeTrackingMode ?? configuration.DefaultChangeTrackingMode);

            mergedConfig.SetDefaultConnectionName(_defaultConnectionName ?? configuration.DefaultConnectionName);

            mergedConfig.SetDefaultCacheRepresentation(_defaultL1CacheRepresentation ?? configuration.DefaultCacheRepresentation);

            mergedConfig.SetDefaultMaterializationMode(_defaultMaterializationMode ?? configuration.DefaultMaterializationMode);

            mergedConfig.SetDefaultSerializationMode(_defaultSerializationMode ?? configuration.DefaultSerializationMode);

            mergedConfig.SetExecutionContext(ExecutionContext ?? configuration.ExecutionContext);

            mergedConfig.SetGenerateDeleteSql(GenerateDeleteSql || configuration.GenerateDeleteSql);

            mergedConfig.SetGenerateInsertSql(GenerateInsertSql || configuration.GenerateInsertSql);

            mergedConfig.SetGenerateUpdateSql(GenerateUpdateSql || configuration.GenerateUpdateSql);

            mergedConfig.SetLogging(Logging || configuration.Logging);

            mergedConfig.SetOperationNamingConvention(_operationNamingConvention ?? configuration.OperationNamingConvention);

            mergedConfig.SetOperationPrefix(_operationPrefix ?? configuration.OperationPrefix);

            mergedConfig.SetHiLoTableName(_hiLoTableName ?? configuration.HiLoTableName);

            mergedConfig.SetLogProvider(LogProvider ?? configuration.LogProvider);

            mergedConfig.SetAutoTypeCoercion(AutoTypeCoercion || configuration.AutoTypeCoercion);

            mergedConfig.SetIgnoreInvalidParameters(IgnoreInvalidParameters || configuration.IgnoreInvalidParameters);

            mergedConfig.SetPadListExpansion(PadListExpansion || configuration.PadListExpansion);

            mergedConfig.SetSystemConfiguration(SystemConfiguration ?? configuration.SystemConfiguration);

            return mergedConfig;
        }
    }
}
