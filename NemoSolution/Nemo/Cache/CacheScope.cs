using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using Nemo.Cache.Providers;

namespace Nemo.Cache
{
    public class CacheScope : IDisposable
    {
        private const string SCOPE_NAME = "__CacheScope";
        
        private HashSet<string> _itemKeys = new HashSet<string>();

        internal static Stack<CacheScope> Scopes
        {
            get
            {
                var scopes = ExecutionContext.Get(SCOPE_NAME);
                if (scopes == null)
                {
                    scopes = new Stack<CacheScope>();
                    ExecutionContext.Set(SCOPE_NAME, scopes);
                }
                return (Stack<CacheScope>)scopes;
            }
        }

        public static CacheScope Current
        {
            get
            {
                var scopes = CacheScope.Scopes;
                if (scopes != null && scopes.Count > 0)
                {
                    return scopes.Peek();
                }
                return null;
            }
        }

        public string Id
        {
            get;
            private set;
        }

        public CacheScope(bool buffered = true, Type cacheType = null, CacheOptions options = null, QueryDependency[] dependencies = null) 
        {
            Id = Guid.NewGuid().ToString();

            options = options ?? new CacheOptions();
            if (!string.IsNullOrEmpty(options.Namespace))
            {
                options.Namespace = Id + "::" + options.Namespace;
            }
            else
            {
                options.Namespace = Id;
            }

            Provider = CacheFactory.GetProvider(cacheType, options);
            Buffered = buffered;
            if (Buffered)
            {
                // No reason to buffer in-process cache
                if (Provider != null && !Provider.IsOutOfProcess)
                {
                    Buffered = false;
                }
            }
            Dependencies = dependencies;
            CacheScope.Scopes.Push(this);
        }

        internal bool Track(string itemKey)
        {
            if (Provider != null)
            {
                return _itemKeys.Add(itemKey);
            }
            return false;
        }

        public void Dispose()
        {
            if (Provider != null)
            {
                foreach (var itemKey in _itemKeys)
                {
                    Provider.Remove(itemKey);
                }
            }

            if (Buffered)
            {
                var bufferProvider = new ExecutionContextCacheProvider();
                foreach (var itemKey in _itemKeys)
                {
                    bufferProvider.Remove(itemKey);
                }
            }

            CacheScope.Scopes.Pop();
        }

        public bool Enabled
        {
            get
            {
                return Provider != null;
            }
        }

        public bool Buffered
        {
            get;
            private set;
        }

        public Type Type
        {
            get
            {
                if (Provider != null)
                {
                    return Provider.GetType();
                }
                return null;
            }
        }

        public CacheProvider Provider
        {
            get;
            private set;
        }

        public HashAlgorithmName HashAlgorithm
        {
            get;
            private set;
        }

        public QueryDependency[] Dependencies
        {
            get;
            private set;
        }
    }
}
