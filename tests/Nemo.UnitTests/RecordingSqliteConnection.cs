using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

// Nemo resolves the dialect provider from the connection type's full name,
// so the fake connection has to be named like a supported provider.
namespace Microsoft.Data.Sqlite
{
    internal sealed class SqliteConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public CancellationToken? OpenToken { get; set; }
        public CancellationToken? NonQueryToken { get; set; }
        public CancellationToken? ScalarToken { get; set; }

        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            OpenToken = cancellationToken;
            _state = ConnectionState.Open;
            return Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => new RecordingCommand(this) { Connection = this };

        private sealed class RecordingCommand : DbCommand
        {
            private readonly SqliteConnection _connection;

            public RecordingCommand(SqliteConnection connection)
            {
                _connection = connection;
            }

            public override string CommandText { get; set; } = string.Empty;
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; }
            protected override DbParameterCollection DbParameterCollection => null;
            protected override DbTransaction DbTransaction { get; set; }

            public override void Cancel() { }
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
            public override int ExecuteNonQuery() => throw new NotSupportedException();
            public override object ExecuteScalar() => throw new NotSupportedException();

            public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            {
                _connection.NonQueryToken = cancellationToken;
                return Task.FromResult(0);
            }

            public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
            {
                _connection.ScalarToken = cancellationToken;
                return Task.FromResult<object>(1);
            }
        }
    }
}
