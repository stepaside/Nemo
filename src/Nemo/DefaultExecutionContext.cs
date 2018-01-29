using System;
using System.Collections.Generic;
using System.Linq;
#if !NETCOREAPP2_0
using System.Runtime.Remoting.Messaging;
#endif
using System.Threading;

namespace Nemo
{
    public sealed class DefaultExecutionContext : IExecutionContext
    {
        private static readonly Lazy<DefaultExecutionContext> LazyCurrent = new Lazy<DefaultExecutionContext>(() => new DefaultExecutionContext(), true);

        public static DefaultExecutionContext Current => LazyCurrent.Value;

        public bool Exists(string name)
        {
            var principal = Thread.CurrentPrincipal as ThreadedPrincipal;
            return principal?.Items.ContainsKey(name) ?? CallContext.LogicalGetData(name) != null;
        }

        public object Get(string name)
        {
            if (!(Thread.CurrentPrincipal is ThreadedPrincipal principal)) return CallContext.LogicalGetData(name);
            principal.Items.TryGetValue(name, out object value);
            return value;
        }

        public bool TryGet(string name, out object value)
        {
            if (Thread.CurrentPrincipal is ThreadedPrincipal principal)
            {
                return principal.Items.TryGetValue(name, out value);
            }
            value = CallContext.LogicalGetData(name);
            return value != null;
        }

        public void Set(string name, object value)
        {
            if (Thread.CurrentPrincipal is ThreadedPrincipal principal)
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
            if (Thread.CurrentPrincipal is ThreadedPrincipal principal)
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
            if (Thread.CurrentPrincipal is ThreadedPrincipal principal)
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
                if (Thread.CurrentPrincipal is ThreadedPrincipal principal)
                {
                    return principal.Items.Keys.ToArray();
                }
                var keys = CallContext.LogicalGetData("$$keys") as HashSet<string>;
                return keys?.ToArray() ?? new string[] { };
            }
        }
    }
}
