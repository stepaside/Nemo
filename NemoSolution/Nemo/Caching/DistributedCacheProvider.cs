using System;
using System.Collections.Generic;
using System.Threading;
using Nemo.Caching.Providers;
using Nemo.Utilities;
using Nemo.Fn;

namespace Nemo.Caching
{
    public abstract class DistributedCacheProvider<TLockManager> : CacheProvider, IDistributedCacheProvider
        where TLockManager : CacheProvider, IDistributedCacheProvider, IDistributedCounter
    {
        private static object _cacheLock = new object();
        
        public abstract bool CheckAndSave(string key, object val, ulong cas);
        public abstract Tuple<object, ulong> RetrieveWithCas(string key);
        public abstract IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys);
        public abstract bool Touch(string key, TimeSpan lifeSpan);
        public abstract object RetrieveStale(string key);
        public abstract IDictionary<string, object> RetrieveStale(IEnumerable<string> keys);
        
        public Tuple<object, bool> RetrieveAndTouch(string key, TimeSpan lifeSpan)
        {
            var result = Retrieve(key);
            var success = false;
            if (result != null)
            {
                success = Touch(key, lifeSpan);
            }
            return Tuple.Create(result, success);
        }

        private const int LOCK_DEFAULT_RETRIES = 4;
        private const double LOCK_DEFAULT_MAXDELAY = 0.7;
        
        protected DistributedCacheProvider(TLockManager lockManager, CacheType cacheType, CacheOptions options)
            : base(cacheType, options)
        {
            LockManager = lockManager;
            LockManager.LifeSpan = TimeSpan.FromMinutes(ObjectFactory.Configuration.DistributedLockTimeout);
        }

        protected TLockManager LockManager
        {
            get;
            private set;
        }

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

            var isStaleCacheEnabled = ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache;
            
            var originalKey = key;
            key = ComputeKey(key);
            key = (isStaleCacheEnabled ? "STALE::" : "LOCK::") + key;

            var stored = false;
            if (ObjectFactory.Configuration.DistributedLockVerification)
            {
                // Value is a combination of the machine name, thread id and tandom value
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
            if (count < 0) count = LOCK_DEFAULT_RETRIES;

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
                    sleepTime = TimeSpan.FromSeconds(Math.Min(0.1 * ((ulong)LockManager.Retrieve("LOCK::" + key) - 0.5), LOCK_DEFAULT_MAXDELAY));
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

            var isStaleCacheEnabled = ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache;

            var originalKey = key;
            key = ComputeKey(key);
            key = (isStaleCacheEnabled ? "STALE::" : "LOCK::") + key;

            var removed = LockManager.Clear(key);
            if (removed)
            {
                if (ObjectFactory.Configuration.DistributedLockVerification)
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
            if (ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache)
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
                if (ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache)
                {
                    return base.ExpiresAt.AddMinutes(ObjectFactory.Configuration.StaleCacheTimeout);
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
                if (ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache)
                {
                    return base.ExpiresAtSpecificTime.Do(d => d.AddMinutes(ObjectFactory.Configuration.StaleCacheTimeout));
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
                if (ObjectFactory.Configuration.CacheContentionMitigation == CacheContentionMitigationType.UseStaleCache)
                {
                    return base.LifeSpan.Add(TimeSpan.FromMinutes(ObjectFactory.Configuration.StaleCacheTimeout));
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
