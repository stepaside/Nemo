using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Nemo.Configuration;
using Nemo.Data;
using Nemo.Extensions;

namespace Nemo.Id
{
    public class HiLoGenerator : IIdGenerator
    {
        private readonly Type _entityType;
        private readonly PropertyInfo _property;
        private readonly string _connectionName;
        private readonly int _maxLo;
        private long _currentHi;
        private int _currentLo;
        private bool _hasTable;

        private readonly object _locker = new object();
        private const string GenerateSql = @"SELECT next_hi FROM {1}{0}{2} WHERE entity_type = {3}{4}; UPDATE {1}{0}{2} SET next_hi = next_hi + 1 WHERE entity_type = {3}{5} AND next_hi = {3}{6};";
        
        public HiLoGenerator(object entity, PropertyInfo property)
            : this(entity, property, 1000)
        {
        }

        public HiLoGenerator(object entity, PropertyInfo property, int maxLo)
            : this(entity, property, maxLo, null)
        {
        }

        public HiLoGenerator(object entity, PropertyInfo property, int maxLo, string connectionName)
            : this(entity.GetType(), property, maxLo, connectionName)
        {
        }

        public HiLoGenerator(Type entityType, PropertyInfo property) 
            : this(entityType, property, 1000)
        {
        }

        public HiLoGenerator(Type entityType, PropertyInfo property, int maxLo)
            : this(entityType, property, maxLo, null)
        {
        }

        public HiLoGenerator(Type entityType, PropertyInfo property, int maxLo, string connectionName)
        {
            _entityType = entityType;
            _property = property;
            _maxLo = maxLo;
            _connectionName = connectionName;
            _currentHi = -1;
        }

        public object Generate()
        {
            long result;
            lock (_locker)
            {
                if (_currentHi == -1)
                {
                    MoveNextHi();
                }
                if (_currentLo == _maxLo)
                {
                    _currentLo = 0;
                    MoveNextHi();
                }
                result = (_currentHi * _maxLo) + _currentLo;
                _currentLo++;
            }
            return Convert.ChangeType(result, _property.PropertyType);
        }

        private void MoveNextHi()
        {
            if (!_hasTable)
            {
                CreateTableIfNotExists();
                _hasTable = true;
            }

            var config = ConfigurationFactory.Get(_entityType);
            var connectionName = _connectionName ?? config.DefaultConnectionName;
            var dialect = DialectFactory.GetProvider(connectionName);
            
            using (var connection = DbFactory.CreateConnection(connectionName, _entityType))
            {
                connection.Open();
                using (var tx = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = tx;
                        command.CommandText = string.Format(GenerateSql, config.HiLoTableName, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter, dialect.VariablePrefix,
                            dialect.UseOrderedParameters ? "?" : "entityType", dialect.UseOrderedParameters ? "?" : "entityType", dialect.UseOrderedParameters ? "?" : "currentHi");
                        command.CommandType = CommandType.Text;
                        if (dialect.UseOrderedParameters)
                        {
                            var p1 = command.CreateParameter();
                            p1.Value = _entityType.Name;
                            var p2 = command.CreateParameter();
                            p2.Value = _entityType.Name;
                            var p3 = command.CreateParameter();
                            p3.Value = _currentHi;

                            command.Parameters.AddRange(new[] { p1, p2, p3 });
                        }
                        else
                        {
                            var p1 = command.CreateParameter();
                            p1.ParameterName = "entityType";
                            p1.Value = _entityType.Name;
                            var p2 = command.CreateParameter();
                            p2.ParameterName = "currentHi";
                            p2.Value = _entityType.Name;

                            command.Parameters.AddRange(new[] { p1, p2 });
                        }

                        _currentHi = (long)command.ExecuteScalar();

                        tx.Commit();
                    }
                    connection.Close();
                }
            }
        }

        private void CreateTableIfNotExists()
        {
            var config = ConfigurationFactory.Get(_entityType);
            var connectionName = _connectionName ?? config.DefaultConnectionName;
            var dialect = DialectFactory.GetProvider(connectionName);

            using (var connection = DbFactory.CreateConnection(connectionName, _entityType))
            {
                connection.Open();
                using (var tx = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = tx;
                        command.CommandText = dialect.CreateTableIfNotExists(config.HiLoTableName,
                            new Dictionary<string, Tuple<DbType, int>> { { "next_hi", Tuple.Create(DbType.Int64, 0) }, { "entity_type", Tuple.Create(DbType.AnsiString, 128) } });
                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                        tx.Commit();
                    }
                    connection.Close();
                }
            }
        }

    }
}
