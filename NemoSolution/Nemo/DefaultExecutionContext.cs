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
        [ThreadStatic]
        private readonly static Dictionary<string, object> _callContext = new Dictionary<string, object>();

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

        public object Get(string name)
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

        public bool TryGet(string name, out object value)
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

        public void Set(string name, object value)
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

        public void Remove(string name)
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

        public object Pop(string name)
        {
            var result = Get(name);
            Remove(name);
            return result;
        }

        public void Clear()
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

        public IList<string> AllKeys
        {
            get
            {
                var principal = Thread.CurrentPrincipal;
                if (principal is ThreadedPrincipal)
                {
                    return ((ThreadedPrincipal)principal).Items.Keys.ToArray();
                }
                else
                {
                    return _callContext.Keys.ToArray();
                }
            }
        }
    }
}
