using Nemo.Cache.Providers;
using Nemo.Cache;
using Nemo.Configuration;
using Nemo.Fn;
using Nemo.Serialization;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Nemo.Cache
{
    public abstract class DistributedCacheProvider : CacheProvider
    {
        protected DistributedCacheProvider(CacheOptions options)
            : base(options)
        { }

        protected IDictionary<string, object> LocalCache
        {
            get
            {
                if (!ExecutionContext.Exists("__LocalCache"))
                {
                    ExecutionContext.Set("__LocalCache", new ConcurrentDictionary<string, object>());
                }
                return (IDictionary<string, object>)ExecutionContext.Get("__LocalCache");
            }
            set
            {
                ExecutionContext.Set("__LocalCache", value);
            }
        }
                
        protected byte[] ComputeValue(CacheValue value, DateTimeOffset currentDateTime)
        {
            if (this is IStaleCacheProvider)
            {
                switch (ExpirationType)
                {
                    case CacheExpirationType.TimeOfDay:
                        value.ExpiresAt = base.ExpiresAtSpecificTime.Value.DateTime;
                        break;

                    case CacheExpirationType.Absolute:
                        value.ExpiresAt = base.ExpiresAt.DateTime;
                        break;

                    case CacheExpirationType.Sliding:
                        value.ExpiresAt = currentDateTime.Add(LifeSpan).DateTime;
                        break;

                    default:
                        value.ExpiresAt = DateTime.MaxValue;
                        break;
                }
            }
            
            return value.ToBytes();
        }

        public override DateTimeOffset ExpiresAt
        {
            get
            {
                if (this is IStaleCacheProvider)
                {
                    return base.ExpiresAt.AddMinutes(ConfigurationFactory.Configuration.StaleCacheTimeout);
                }
                else
                {
                    return base.ExpiresAt;
                }
            }
        }

        public override DateTimeOffset? ExpiresAtSpecificTime
        {
            get
            {
                if (this is IStaleCacheProvider)
                {
                    if (base.ExpiresAtSpecificTime != null)
                    {
                        return base.ExpiresAtSpecificTime.Value.AddMinutes(ConfigurationFactory.Configuration.StaleCacheTimeout);
                    }
                    else
                    {
                        return base.ExpiresAtSpecificTime;
                    }
                }
                else
                {
                    return base.ExpiresAtSpecificTime;
                }
            }
        }

        public override TimeSpan LifeSpan
        {
            get
            {
                if (this is IStaleCacheProvider)
                {
                    return base.LifeSpan.Add(TimeSpan.FromMinutes(ConfigurationFactory.Configuration.StaleCacheTimeout));
                }
                else
                {
                    return base.LifeSpan;
                }
            }
        }

        public abstract bool TryAcquireLock(string key);
        
        public abstract object WaitForItems(string key, int count = -1);
        
        public abstract bool ReleaseLock(string key);
    }
}
