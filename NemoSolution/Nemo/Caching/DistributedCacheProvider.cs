using Nemo.Caching.Providers;
using Nemo.Configuration;
using Nemo.Fn;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Nemo.Caching
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

        protected byte[] ExtractValue(object value)
        {
            return value is CacheItem ? ((CacheItem)value).Data : value as byte[];
        }

        protected byte[] ComputeValue(byte[] value, DateTimeOffset currentDateTime)
        {
            if (this is IStaleCacheProvider)
            {
                switch (ExpirationType)
                {
                    case CacheExpirationType.TimeOfDay:
                        {
                            var t = new TemporalValue { Value = value, ExpiresAt = base.ExpiresAtSpecificTime.Value.DateTime };
                            return t.ToBytes();
                        }
                    case CacheExpirationType.DateTime:
                        {
                            var t = new TemporalValue { Value = value, ExpiresAt = base.ExpiresAt.DateTime };
                            return t.ToBytes();
                        }
                    case CacheExpirationType.TimeSpan:
                        {
                            var t = new TemporalValue { Value = value, ExpiresAt = currentDateTime.Add(LifeSpan).DateTime };
                            return t.ToBytes();
                        }
                    default:
                        {
                            var t = new TemporalValue { Value = value, ExpiresAt = DateTime.MaxValue };
                            return t.ToBytes();
                        }
                }
            }
            else
            {
                return value;
            }
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
            protected internal set
            {
                base.ExpiresAt = value;
            }
        }

        public override Maybe<DateTimeOffset> ExpiresAtSpecificTime
        {
            get
            {
                if (this is IStaleCacheProvider)
                {
                    return base.ExpiresAtSpecificTime.Do(d => d.AddMinutes(ConfigurationFactory.Configuration.StaleCacheTimeout));
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
            protected internal set
            {
                base.LifeSpan = value;
            }
        }

        public abstract bool TryAcquireLock(string key);
        
        public abstract object WaitForItems(string key, int count = -1);
        
        public abstract bool ReleaseLock(string key);
    }
}
