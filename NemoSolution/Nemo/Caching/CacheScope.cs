using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;

namespace Nemo.Caching
{
    public class CacheScope : IDisposable
    {
        private const string SCOPE_NAME = "CACHE_SCOPE";
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

        public CacheScope(bool buffered = true, CacheType cacheType = CacheType.None, CacheOptions options = null, CacheLink[] links = null) 
        {
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
            HashAlgorithm = options != null && options.HashAlgorithm.HasValue ? options.HashAlgorithm.Value : HashAlgorithmName.Default;
            Links = links;
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
                    Provider.Clear(itemKey);
                }
            }

            if (Buffered)
            {
                var bufferProvider = CacheFactory.GetProvider(CacheType.ExecutionContext);
                foreach (var itemKey in _itemKeys)
                {
                    bufferProvider.Clear(itemKey);
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

        public CacheType Type
        {
            get
            {
                if (Provider != null)
                {
                    return Provider.Type;
                }
                return CacheType.None;
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

        public CacheLink[] Links
        {
            get;
            private set;
        }
    }
}
