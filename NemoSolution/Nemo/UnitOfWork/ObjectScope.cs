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
        private const string SCOPE_NAME = "__ObjectScope";
        internal IBusinessObject Item = null;
        internal byte[] ItemSnapshot = null;
        internal IBusinessObject OriginalItem = null;
        internal readonly Type ItemType = null;
        private bool? _hasException = null;

        internal static Stack<ObjectScope> Scopes
        {
            get
            {
                var scopes = ExecutionContext.Get(SCOPE_NAME);
                if (scopes == null)
                {
                    scopes = new Stack<ObjectScope>();
                    ExecutionContext.Set(SCOPE_NAME, scopes);
                }
                return (Stack<ObjectScope>)scopes;
            }
        }

        public static ObjectScope Current
        {
            get
            {
                return ObjectScope.Scopes.FirstOrDefault();
            }
        }

        static byte[] CreateSnapshot(IBusinessObject item)
        {
            return item.Serialize(SerializationMode.SerializeAll);
        }

        public static ObjectScope New<T>(T item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default)
            where T : class, IBusinessObject
        {
            return new ObjectScope(item, autoCommit, mode, typeof(T));
        }

        private ObjectScope(IBusinessObject item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default, Type type = null)
        {
            if (item == null && type == null)
            {
                throw new ArgumentException("Invalid ObjectScope definition");
            }

            if (item != null)
            {
                item.CheckReadOnly();
            }

            this.AutoCommit = autoCommit;
            this.IsNew = item == null;
            ItemType = type;
            this.ChangeTracking = mode != ChangeTrackingMode.Default ? mode : ConfigurationFactory.Configuration.DefaultChangeTrackingMode;
            if (!this.IsNew)
            {
                if (type == null)
                {
                    ItemType = item.GetType();
                }
                Item = item;
                ItemSnapshot = CreateSnapshot(item);
            }
            ObjectScope.Scopes.Push(this);
            Transaction = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted });
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
                return ObjectScope.Scopes.Count > 1;
            }
        }

        internal TransactionScope Transaction
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

        internal bool UpdateOuterSnapshot<T>(T businessObject)
            where T : class, IBusinessObject
        {
            return UpdateSnapshot<T>(businessObject, 1);
        }

        internal bool UpdateCurrentSnapshot<T>(T businessObject)
            where T : class, IBusinessObject
        {
            return UpdateSnapshot<T>(businessObject, 0);
        }

        private bool UpdateSnapshot<T>(T businessObject, int index)
            where T : class, IBusinessObject
        {
            var outerScope = ObjectScope.Scopes.ElementAtOrDefault(index);
            if (outerScope != null)
            {
                if (outerScope.Item == businessObject)
                {
                    outerScope.ItemSnapshot = CreateSnapshot(businessObject);
                    outerScope.OriginalItem = null;
                    return true;
                }
            }
            return false;
        }

        public void Dispose()
        {
            if (this.AutoCommit)
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
            ObjectScope.Scopes.Pop();
        }
    }
}
