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
    internal sealed class ExecutionContext
    {
        [ThreadStatic]
        private static Dictionary<string, object> _callContext = new Dictionary<string, object>();

        private ExecutionContext()
        {
            throw new NotSupportedException("must not be instantiated");
        }

        internal static bool Exists(string name)
        {
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                return ((ThreadedPrincipal)principal).Items.ContainsKey(name);
            }
            else
            {
                return _callContext.ContainsKey(name);
            }
        }

        internal static object Get(string name)
        {
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                object value;
                ((ThreadedPrincipal)principal).Items.TryGetValue(name, out value);
                return value;
            }
            else
            {
                object value;
                _callContext.TryGetValue(name, out value);
                return value;
            }
        }

        internal static bool TryGet(string name, out object value)
        {
            value = null;
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                return ((ThreadedPrincipal)principal).Items.TryGetValue(name, out value);
            }
            else
            {
                return _callContext.TryGetValue(name, out value);
            }
        }

        internal static void Set(string name, object value)
        {
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                ((ThreadedPrincipal)principal).Items[name] = value;
            }
            else
            {
                _callContext[name] = value;
            }
        }

        internal static void Remove(string name)
        {
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                ((ThreadedPrincipal)principal).Items.Remove(name);
            }
            else
            {
                _callContext.Remove(name);
            }
        }

        internal static object Pop(string name)
        {
            var result = Get(name);
            Remove(name);
            return result;
        }

        internal static void Clear()
        {
            var principal = Thread.CurrentPrincipal;
            if (principal is ThreadedPrincipal)
            {
                ((ThreadedPrincipal)principal).Items.Clear();
            }
            else
            {
                _callContext.Clear();
            }
        }
    }
}
