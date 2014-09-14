using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Runtime.Remoting.Messaging;
using System.Collections.Concurrent;
using System.Threading;

namespace Nemo
{
    public sealed class DefaultExecutionContext : IExecutionContext
    {
        private readonly static Lazy<DefaultExecutionContext> _current = new Lazy<DefaultExecutionContext>(() => new DefaultExecutionContext(), true);

        public static DefaultExecutionContext Current
        {
            get
            {
                return _current.Value;
            }
        }

        public bool Exists(string name)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            return principal != null ? principal.Items.ContainsKey(name) : CallContext.LogicalGetData(name) != null;
        }

        public object Get(string name)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            if (principal != null)
            {
                object value;
                principal.Items.TryGetValue(name, out value);
                return value;
            }
            return CallContext.LogicalGetData(name);
        }

        public bool TryGet(string name, out object value)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            if (principal != null)
            {
                return principal.Items.TryGetValue(name, out value);
            }
            value = CallContext.LogicalGetData(name);
            return value != null;
        }

        public void Set(string name, object value)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            if (principal != null)
            {
                principal.Items[name] = value;
            }
            else
            {
                var keys = CallContext.LogicalGetData("$$keys") as HashSet<string>;
                if (keys == null)
                {
                    keys = new HashSet<string>();
                    CallContext.LogicalSetData("$$keys", keys);
                }
                keys.Add(name);
                CallContext.LogicalSetData(name, value);
            }
        }

        public void Remove(string name)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            if (principal != null)
            {
                principal.Items.Remove(name);
            }
            else
            {
                CallContext.FreeNamedDataSlot(name);
            }
        }

        public object Pop(string name)
        {
            var result = Get(name);
            Remove(name);
            return result;
        }

        public void Clear()
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            if (principal != null)
            {
                principal.Items.Clear();
            }
            else
            {
                var keys = CallContext.LogicalGetData("$$keys") as HashSet<string>;
                if (keys == null) return;
                foreach (var key in keys)
                {
                    CallContext.FreeNamedDataSlot(key);
                }
                keys.Clear();
            }
        }

        public IList<string> AllKeys
        {
            get
            {
                var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
                if (principal != null)
                {
                    return principal.Items.Keys.ToArray();
                }
                var keys = CallContext.LogicalGetData("$$keys") as HashSet<string>;
                return keys == null ? new string[] { } : keys.ToArray();
            }
        }
    }
}
