using System;
using System.Collections.Generic;
using System.Threading;
using Nemo.Caching.Providers;
using Nemo.Utilities;

namespace Nemo.Caching
{
    public abstract class DistributedCacheProvider<TLockManager> : CacheProvider, IDistributedCacheProvider
        where TLockManager : CacheProvider, IDistributedCacheProvider
    {
        private static object _cacheLock = new object();

        public abstract bool CheckAndSave(string key, object val, ulong cas);
        public abstract Tuple<object, ulong> RetrieveWithCas(string key);
        public abstract IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys);
        public abstract bool Touch(string key, TimeSpan lifeSpan);

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
        private const int LOCK_DEFAULT_EXPIRES = 2;

        protected DistributedCacheProvider(TLockManager lockManager, CacheType cacheType, CacheOptions options)
            : base(cacheType, options)
        {
            LockManager = lockManager;
            LockManager.LifeSpan = TimeSpan.FromMinutes(LOCK_DEFAULT_EXPIRES);
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

            var originalKey = key;
            key = ComputeKey(key);

            key = "LOCK::" + key;

            var stored = false;
            if (ObjectFactory.Configuration.DistributedLockVerification)
            {
                // Value is a combination of the machine name, thread id and 
                var value = Environment.MachineName + "::" + DateTime.Now.Ticks + "::" + new Random().NextDouble();
                stored = LockManager.AddNew(key, value);
                if (stored)
                {
                    stored = value == (string)LockManager.Retrieve(key);
                }
            }
            else
            {
                lock (_cacheLock)
                {
                    stored = LockManager.AddNew(key, 1);
                }
            }

            if (stored)
            {
                Log.Capture(() => string.Format("Acquired lock for {0}", originalKey));
            }
            else
            {
                Log.Capture(() => string.Format("Failed to acquire lock for {0}", originalKey));
            }
            return stored;
        }

        public object WaitForItems(string key, int count = -1)
        {
            if (count < 0) count = LOCK_DEFAULT_RETRIES;

            object result = null;
            double totalSleepTime = 0;
            count = count <= 0 ? 1 : count;
            for (int i = 0; i < count; i++)
            {
                // Check if items have been added thus the lock has been released 
                result = Retrieve(key);
                if (result != null)
                {
                    break;
                }

                var sleepTime = TimeSpan.FromSeconds(Math.Pow(2, i) / 3.5);
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

            var originalKey = key;
            key = ComputeKey(key);

            key = "LOCK::" + key;

            var removed = LockManager.Clear(key);
            if (removed)
            {
                Log.Capture(() => string.Format("Removed lock for {0}", originalKey));
            }
            else
            {
                Log.Capture(() => string.Format("Failed to remove lock for {0}", originalKey));
            }
            return removed;
        }
    }
}
