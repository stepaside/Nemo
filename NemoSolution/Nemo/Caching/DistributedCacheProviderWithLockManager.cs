using Nemo.Caching.Providers;
using Nemo.Configuration;
using Nemo.Fn;
using Nemo.Utilities;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Nemo.Caching
{
    public abstract class DistributedCacheProviderWithLockManager<TLockManager> : CacheProvider, IDistributedCacheProvider
        where TLockManager : CacheProvider, IDistributedCacheProvider, IDistributedCounter
    {
        private readonly int _distributedLockRetryCount = ConfigurationFactory.Configuration.DistributedLockRetryCount;
        private readonly double _distributedLockWaitTime = ConfigurationFactory.Configuration.DistributedLockWaitTime;
        
        protected DistributedCacheProviderWithLockManager(TLockManager lockManager, CacheOptions options)
            : base(options)
        {
            LockManager = lockManager;
            LockManager.LifeSpan = TimeSpan.FromMinutes(ConfigurationFactory.Configuration.DistributedLockTimeout);
        }

        protected TLockManager LockManager
        {
            get;
            private set;
        }

        public abstract object RetrieveUsingRawKey(string key);

        public void AcquireLock(string key)
        {
            if (!TryAcquireLock(key))
            {
                throw new ApplicationException(string.Format("Could not acquire lock for '{0}'", key));
            }
        }

        public bool TryAcquireLock(string key)
        {
            if (LockManager == null)
            {
                return true;
            }

            var isStaleCacheEnabled = ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider;
            
            var originalKey = key;
            key = ComputeKey(key);
            key = (isStaleCacheEnabled ? "STALE::" : "LOCK::") + key;

            var stored = false;
            if (ConfigurationFactory.Configuration.DistributedLockVerification)
            {
                // Value is a combination of the machine name, thread id and random value
                var ticket = "TICKET::" + key;
                var value = Environment.MachineName + "::" + Thread.CurrentThread.ManagedThreadId + "::" + DateTime.Now.Ticks + "::" + new Random().NextDouble();
                stored = LockManager.AddNew(ticket, value);
                if (stored)
                {
                    stored = value == (string)LockManager.Retrieve(ticket);
                    if (stored)
                    {
                        LockManager.Increment(key);
                    }
                }
            }
            else
            {
                stored = LockManager.AddNew(key, 1);
            }

            if (stored)
            {
                Log.Capture(() => string.Format("Acquired lock for {0}", originalKey));
            }
            else
            {
                if (!isStaleCacheEnabled)
                {
                    LockManager.Increment(key);
                }
                Log.Capture(() => string.Format("Failed to acquire lock for {0}", originalKey));
            }
            return stored;
        }

        public object WaitForItems(string key, int count = -1)
        {
            if (count < 0) count = _distributedLockRetryCount;

            object result = null;
            double totalSleepTime = 0;
            var sleepTime = TimeSpan.Zero;
            count = count <= 0 ? 1 : count;
            for (int i = 0; i < count; i++)
            {
                // Check if items have been added thus the lock has been released 
                result = Retrieve(key);
                if (result != null)
                {
                    break;
                }

                if (sleepTime == TimeSpan.Zero)
                {
                    sleepTime = TimeSpan.FromSeconds(Math.Min(0.1 * ((ulong)LockManager.RetrieveUsingRawKey("LOCK::" + key) - 0.5), _distributedLockWaitTime));
                }
                else
                {
                    sleepTime = TimeSpan.FromSeconds(sleepTime.TotalSeconds / 2);
                }

                totalSleepTime += sleepTime.TotalSeconds;
                Log.Capture(() => string.Format("Waiting for locked key {0} (sleep time {1} s)", key, sleepTime.TotalSeconds));
                Thread.Sleep(sleepTime);
            }
            Log.Capture(() => string.Format("Finished waiting for locked key {0} (value is {2}present, total wait time {1} s)", key, totalSleepTime, result == null ? "not " : string.Empty));
            return result;
        }

        public bool ReleaseLock(string key)
        {
            if (LockManager == null)
            {
                return true;
            }

            var isStaleCacheEnabled = ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider;

            var originalKey = key;
            key = ComputeKey(key);
            key = (isStaleCacheEnabled ? "STALE::" : "LOCK::") + key;

            var removed = LockManager.Clear(key);
            if (removed)
            {
                if (ConfigurationFactory.Configuration.DistributedLockVerification)
                {
                    LockManager.Clear("TICKET::" + key);
                }
                Log.Capture(() => string.Format("Removed lock for {0}", originalKey));
            }
            else
            {
                Log.Capture(() => string.Format("Failed to remove lock for {0}", originalKey));
            }
            return removed;
        }

        protected object ComputeValue(object value, DateTimeOffset currentDateTime)
        {
            if (ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider)
            {
                switch (ExpirationType)
                {
                    case CacheExpirationType.TimeOfDay:
                        return new TemporalValue { Value = value, ExpiresAt = base.ExpiresAtSpecificTime.Value.DateTime };
                    case CacheExpirationType.DateTime:
                        return new TemporalValue { Value = value, ExpiresAt = base.ExpiresAt.DateTime };
                    case CacheExpirationType.TimeSpan:
                        return new TemporalValue { Value = value, ExpiresAt = currentDateTime.Add(LifeSpan).DateTime };
                    default:
                        return value;
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
                if (ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider)
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
                if (ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider)
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
                if (ConfigurationFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache && this is IStaleCacheProvider)
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
    }
}
