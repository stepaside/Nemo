using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using Nemo.Extensions;
using Nemo.Serialization;
using System.Transactions;
using Nemo.Reflection;
using System.Collections;
using Nemo.Attributes;
using System.Data.Common;
using Nemo.Data;
using System.Data;

namespace Nemo.UnitOfWork
{
    public class ObjectScope : IDisposable
    {
        private const string SCOPE_NAME = "OBJECT_SCOPE";
        internal readonly Dictionary<IBusinessObject, byte[]> Snapshots = new Dictionary<IBusinessObject, byte[]>();
        internal readonly Dictionary<IBusinessObject, object> DeserializedSnapshots = new Dictionary<IBusinessObject, object>();
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

        public ObjectScope(IBusinessObject item = null, bool autoCommit = false, ChangeTrackingMode mode = ChangeTrackingMode.Default)
        {
            if (item != null)
            {
                item.CheckReadOnly();
            }

            this.AutoCommit = autoCommit;
            this.IsNew = item == null;
            this.ChangeTracking = mode != ChangeTrackingMode.Default ? mode : ObjectFactory.Configuration.DefaultChangeTrackingMode;
            if (!this.IsNew)
            {
                this.Snapshots.Add(item, CreateSnapshot(item));
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
                if (outerScope.Snapshots.ContainsKey(businessObject))
                {
                    outerScope.Snapshots[businessObject] = CreateSnapshot(businessObject);
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

                foreach (var item in this.Snapshots.Keys)
                {
                    if (_hasException.Value || !item.Commit())
                    {
                        item.Rollback();
                    }
                }
            }
            Snapshots.Clear();
            Transaction.Dispose();
            ObjectScope.Scopes.Pop();
        }
    }
}
