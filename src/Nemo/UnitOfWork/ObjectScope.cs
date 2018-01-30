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
using System.Transactions;

namespace Nemo.UnitOfWork
{
    public class ObjectScope : IDisposable
    {
        private const string ScopeNameStore = "__ObjectScope";
        internal object Item = null;
        internal byte[] ItemSnapshot = null;
        internal object OriginalItem = null;
        internal readonly Type ItemType = null;
        private bool? _hasException = null;

        internal static Stack<ObjectScope> Scopes
        {
            get
            {
                var scopes = ConfigurationFactory.DefaultConfiguration.ExecutionContext.Get(ScopeNameStore);
                if (scopes == null)
                {
                    scopes = new Stack<ObjectScope>();
                    ConfigurationFactory.DefaultConfiguration.ExecutionContext.Set(ScopeNameStore, scopes);
                }
                return (Stack<ObjectScope>)scopes;
            }
        }

        public static ObjectScope Current
        {
            get
            {
                return Scopes.FirstOrDefault();
            }
        }

        static byte[] CreateSnapshot(object item)
        {
            return item.Serialize(SerializationMode.SerializeAll);
        }

        public static ObjectScope New<T>(T item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default, DbConnection connection = null)
            where T : class
        {
            return new ObjectScope(item, autoCommit, mode, typeof(T), connection);
        }
        
        private ObjectScope(object item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default, Type type = null, DbConnection connection = null)
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
            Scopes.Push(this);
            if (connection == null || connection.State != ConnectionState.Open)
            {
                Transaction = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted });
            }
            else
            {
                Connection = connection;
            }
        }
        
        public bool AutoCommit
        {
            get;
            private set;
        }

        public ChangeTrackingMode ChangeTracking
        {
            get;
            private set;
        }

        public bool IsNew
        {
            get;
            private set;
        }

        internal bool IsNested
        {
            get
            {
                return Scopes.Count > 1;
            }
        }

        internal TransactionScope Transaction
        {
            get;
            private set;
        }

        internal DbConnection Connection
        {
            get;
            private set;
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
            var outerScope = Scopes.ElementAtOrDefault(index);
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
            if (AutoCommit)
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
            Transaction.Dispose();
            Scopes.Pop();
        }
    }
}
