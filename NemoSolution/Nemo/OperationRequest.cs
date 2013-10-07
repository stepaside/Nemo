using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using Nemo.Extensions;
using System.Collections.Generic;

namespace Nemo
{
    [Serializable]
    public class OperationRequest
    {
        private string _connectionString = null;
        private IList<Param> _parameters = new List<Param>();
        private OperationReturnType _returnType = OperationReturnType.Guess;
        private OperationType _operationType = OperationType.Guess;
        
        [NonSerialized]
        private DbConnection _connection = null;
        [NonSerialized]
        private DbTransaction _transaction = null;

        public string Operation { get; set; }

        public string ConnectionName { get; set; }

        public DbConnection Connection
        {
            get
            {
                if (_connection == null && ConnectionName.NullIfEmpty() == null && _connectionString.NullIfEmpty() != null)
                {
                    _connection = GetConnection(_connectionString);
                }
                return _connection;
            }
            set
            {
                _connection = value;
                if (_connection != null)
                {
                    _connectionString = _connection.ConnectionString;
                }
            }
        }
        
        public DbTransaction Transaction 
        {
            get
            {
                return _transaction;
            }
            set
            {
                _transaction = value;
                Connection = _transaction.Connection;
            }
        }
        
        public bool InTransaction
        {
            get
            {
                return Transaction != null || System.Transactions.Transaction.Current != null;
            }
        }

        public IList<Param> Parameters
        {
            get
            {
                return _parameters;
            }
            set
            {
                if (value != null)
                {
                    _parameters = new List<Param>(value);
                }
            }
        }

        public IList<Type> Types
        {
            get;
            set;
        }

        public OperationReturnType ReturnType
        {
            get
            {
                return _returnType;
            }
            set
            {
                _returnType = value;
            }
        }

        public OperationType OperationType
        {
            get
            {
                return _operationType;
            }
            set
            {
                _operationType = value;
            }
        }

        public bool CaptureException { get; set; }

        public string SchemaName { get; set; }

        public bool IsValid()
        {
            return _transaction != null || _connection != null || !string.IsNullOrEmpty(_connectionString) || (Types != null && Types.Count > 0);
        }
        
        private static DbConnection GetConnection(string connectionString)
        {
            string providerName = null;
            var csb = new DbConnectionStringBuilder { ConnectionString = connectionString };

            if (csb.ContainsKey("provider"))
            {
                providerName = csb["provider"].ToString();
            }
            else
            {
                var css = ConfigurationManager.ConnectionStrings.Cast<ConnectionStringSettings>().FirstOrDefault(x => string.Equals(x.ConnectionString, connectionString, StringComparison.OrdinalIgnoreCase));
                if (css != null) providerName = css.ProviderName;
            }

            if (providerName != null)
            {
                var providerExists = DbProviderFactories.GetFactoryClasses().Rows.Cast<DataRow>().Any(r => r[2].Equals(providerName));
                if (providerExists)
                {
                    var factory = DbProviderFactories.GetFactory(providerName);
                    return factory.CreateConnection();
                }
            }

            return null;
        }
    }
}
