using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Nemo.Cache.Providers
{
    public class ExecutionContextCacheProvider : CacheProvider
    {
        public ExecutionContextCacheProvider(CacheOptions options = null)
            : base(options)
        { }

        public override void Clear()
        {
            ExecutionContext.Clear();
        }

        public override object Pop(string key)
        {
            key = ComputeKey(key);
            return ExecutionContext.Pop(key);
        }

        public override bool Remove(string key)
        {
            key = ComputeKey(key);
            ExecutionContext.Remove(key);
            return true;
        }

        public override bool Add(string key, object val)
        {
            key = ComputeKey(key);
            var success = false;
            if (!ExecutionContext.Exists(key))
            {
                ExecutionContext.Set(key, val);
                success = true;
            }
            return success;
        }

        public override bool Set(string key, object val)
        {
            key = ComputeKey(key);
            ExecutionContext.Set(key, val);
            return true;
        }

        public override bool Set(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            foreach (var k in keys)
            {
                ExecutionContext.Set(k.Key, items[k.Value]);
            }
            return true;
        }

        public override object Get(string key)
        {
            key = ComputeKey(key);
            return ExecutionContext.Get(key);
        }

        public override IDictionary<string, object> Get(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            return computedKeys.ToDictionary(key => key.Value, key => ExecutionContext.Get(key.Key));
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            return false;
        }

        public IList<string> AllKeys
        {
            get
            {
                return ExecutionContext.AllKeys;
            }
        }
    }
}
