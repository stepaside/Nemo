using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Nemo.UnitOfWork
{
    public class ObjectScope : IDisposable, IAsyncDisposable
    {
        private const string ScopeNameStore = "__ObjectScope";
        private bool? _hasException = null;
        private bool _disposed;
        private volatile bool _removed;

        /// <summary>
        /// Immutable stack of scopes. A new head is written to the execution context on every push and removal,
        /// so parallel execution flows never observe or corrupt each other's scopes.
        /// </summary>
        private sealed class ScopeNode
        {
            internal ScopeNode(ObjectScope scope, ScopeNode next)
            {
                Scope = scope;
                Next = next;
            }

            internal ObjectScope Scope { get; }

            internal ScopeNode Next { get; }
        }

        private static ScopeNode Head
        {
            get => ConfigurationFactory.DefaultConfiguration.ExecutionContext.Get(ScopeNameStore) as ScopeNode;
            set => ConfigurationFactory.DefaultConfiguration.ExecutionContext.Set(ScopeNameStore, value);
        }

        private static IEnumerable<ObjectScope> ActiveScopes
        {
            get
            {
                for (var node = Head; node != null; node = node.Next)
                {
                    // A scope removed by another execution flow cannot be unlinked from this flow's head,
                    // because an execution context written inside an async method does not flow back to its caller.
                    if (node.Scope._removed) continue;
                    yield return node.Scope;
                }
            }
        }

        internal static int ScopeCount
        {
            get
            {
                var count = 0;
                for (var node = Head; node != null; node = node.Next)
                {
                    if (!node.Scope._removed) count++;
                }
                return count;
            }
        }

        internal static ObjectScope[] ScopeArray => ActiveScopes.ToArray();

        internal static void ClearScopes()
        {
            Head = null;
        }

        public static ObjectScope Current
        {
            get
            {
                for (var node = Head; node != null; node = node.Next)
                {
                    if (!node.Scope._removed) return node.Scope;
                }
                return null;
            }
        }

        static byte[] CreateSnapshot(object item)
        {
            return item.Serialize(SerializationMode.SerializeAll);
        }

        public static ObjectScope New<T>(T item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default, DbConnection connection = null, INemoConfiguration config = null)
            where T : class
        {
            return new ObjectScope(item, autoCommit, mode, typeof(T), connection, config);
        }
        
        private ObjectScope(object item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default, Type type = null, DbConnection connection = null, INemoConfiguration config = null)
        {
            if (item == null && type == null)
            {
                throw new ArgumentException("Invalid ObjectScope definition");
            }

            if (item != null)
            {
                item.CheckReadOnly();
            }

            AutoCommit = autoCommit;
            IsNew = item == null;
            ItemType = type;
            Configuration = config;
            ChangeTracking = mode != ChangeTrackingMode.Default ? mode : ConfigurationFactory.Get(type).DefaultChangeTrackingMode;
            if (!IsNew)
            {
                if (type == null)
                {
                    ItemType = item.GetType();
                }
                Item = item;
                ItemSnapshot = CreateSnapshot(item);
            }
            Head = new ScopeNode(this, Head);
            if (connection == null || connection.State != ConnectionState.Open)
            {
                Transaction = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted }, TransactionScopeAsyncFlowOption.Enabled);
            }
            else
            {
                Connection = connection;
            }
        }

        internal object Item { get; private set; }
        
        internal byte[] ItemSnapshot { get; private set; }
        
        internal object OriginalItem { get; set; }
        
        internal Type ItemType { get; }

        public bool AutoCommit { get; }

        public ChangeTrackingMode ChangeTracking { get; }

        public bool IsNew { get; }

        internal bool IsNested => ScopeCount > 1;

        internal TransactionScope Transaction { get; }

        internal DbConnection Connection { get; }

        internal INemoConfiguration Configuration { get; }

        /// <summary>
        /// Marks the scope as failed so that an auto-committing scope rolls back on disposal.
        /// Required for asynchronous scopes, where an in-flight exception cannot be detected on disposal.
        /// </summary>
        public void MarkFailed()
        {
            _hasException = true;
        }

        internal void Cleanup()
        {
            Item = null;
            ItemSnapshot = null;
            OriginalItem = null;
        }

        internal bool UpdateOuterSnapshot<T>(T dataEntity)
            where T : class
        {
            return UpdateSnapshot(dataEntity, 1);
        }

        internal bool UpdateCurrentSnapshot<T>(T dataEntity)
            where T : class
        {
            return UpdateSnapshot(dataEntity, 0);
        }

        private bool UpdateSnapshot<T>(T dataEntity, int index)
            where T : class
        {
            var outerScope = ScopeAt(index);
            if (outerScope != null)
            {
                if (outerScope.Item == dataEntity)
                {
                    outerScope.ItemSnapshot = CreateSnapshot(dataEntity);
                    outerScope.OriginalItem = null;
                    return true;
                }
            }
            return false;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                // The scope may still be referenced by another execution flow than the one that disposed it.
                RemoveScope();
                return;
            }
            _disposed = true;

            try
            {
                if (AutoCommit && Item != null)
                {
                    if (_hasException == null)
                    {
                        long exceptionCode = Marshal.GetExceptionCode();
                        _hasException = exceptionCode != 0 && exceptionCode != 0xCCCCCCCC;
                    }

                    if (_hasException.Value || !Item.Commit(ItemType))
                    {
                        Item.Rollback(ItemType);
                    }
                }
            }
            finally
            {
                try
                {
                    Transaction?.Dispose();
                }
                finally
                {
                    RemoveScope();
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                RemoveScope();
                return;
            }
            _disposed = true;

            try
            {
                if (AutoCommit && Item != null)
                {
                    if (_hasException.GetValueOrDefault() || !await Item.CommitAsync(ItemType).ConfigureAwait(false))
                    {
                        Item.Rollback(ItemType);
                    }
                }
            }
            finally
            {
                try
                {
                    Transaction?.Dispose();
                }
                finally
                {
                    RemoveScope();
                }
            }
        }

        private static ObjectScope ScopeAt(int index) => ActiveScopes.ElementAtOrDefault(index);

        private void RemoveScope()
        {
            _removed = true;

            var retained = ActiveScopes.ToArray();
            ScopeNode rebuilt = null;
            for (var i = retained.Length - 1; i >= 0; i--)
            {
                rebuilt = new ScopeNode(retained[i], rebuilt);
            }
            Head = rebuilt;
        }
    }
}
