using System;
using System.Collections.Generic;
using System.Linq;
#if !NETSTANDARD2_0_OR_GREATER && !NETCOREAPP
using System.Runtime.Remoting.Messaging;
#endif
using System.Threading;

namespace Nemo
{
    public sealed class DefaultExecutionContext : IExecutionContext
    {
        private static readonly Lazy<DefaultExecutionContext> LazyCurrent = new Lazy<DefaultExecutionContext>(() => new DefaultExecutionContext(), true);

        private const string LogicalDataKeys = "$$LogicalDataKeys";

        public static DefaultExecutionContext Current => LazyCurrent.Value;

        public bool Exists(string name)
        {
            var principal = Thread.CurrentPrincipal as HttpContextPrincipal;
            return principal?.Items.ContainsKey(name) ?? CallContext.LogicalGetData(name) != null;
        }

        public object Get(string name)
        {
            if (!(Thread.CurrentPrincipal is HttpContextPrincipal principal)) return CallContext.LogicalGetData(name);
            principal.Items.TryGetValue(name, out object value);
            return value;
        }

        public bool TryGet(string name, out object value)
        {
            if (Thread.CurrentPrincipal is HttpContextPrincipal principal)
            {
                return principal.Items.TryGetValue(name, out value);
            }
            value = CallContext.LogicalGetData(name);
            return value != null;
        }

        public void Set(string name, object value)
        {
            if (Thread.CurrentPrincipal is HttpContextPrincipal principal)
            {
                principal.Items[name] = value;
            }
            else
            {
                if (!(CallContext.LogicalGetData(LogicalDataKeys) is HashSet<string> keys))
                {
                    keys = new HashSet<string>();
                    CallContext.LogicalSetData(LogicalDataKeys, keys);
                }
                keys.Add(name);
                CallContext.LogicalSetData(name, value);
            }
        }

        public void Remove(string name)
        {
            if (Thread.CurrentPrincipal is HttpContextPrincipal principal)
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
            if (Thread.CurrentPrincipal is HttpContextPrincipal principal)
            {
                principal.Items.Clear();
            }
            else
            {
                if (!(CallContext.LogicalGetData(LogicalDataKeys) is HashSet<string> keys)) return;
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
                if (Thread.CurrentPrincipal is HttpContextPrincipal principal)
                {
                    return principal.Items.Keys.ToArray();
                }
                var keys = CallContext.LogicalGetData(LogicalDataKeys) as HashSet<string>;
                return keys?.ToArray() ?? new string[] { };
            }
        }
    }
}
