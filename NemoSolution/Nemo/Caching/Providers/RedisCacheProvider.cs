using BookSleeve;
using Nemo.Configuration;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Caching.Providers
{
    public class RedisCacheProvider : DistributedCacheProvider, IRevisionProvider, IPersistentCacheProvider
    {
        #region Static Declarations

        private static readonly Dictionary<string, RedisConnection> _redisConnectionList = new Dictionary<string, RedisConnection>();

        private static readonly object _connectionLock = new object();

        private static readonly int _waitTimeOut = 5000;

        public static RedisConnection GetRedisConnection(string config)
        {
            RedisConnection connection = null;
            config = !string.IsNullOrEmpty(config) ? config : DefaultHostName;
            if (config.NullIfEmpty() != null)
            {
                lock (_connectionLock)
                {
                    if (!_redisConnectionList.TryGetValue(config, out connection))
                    {
                        connection = ConnectionUtils.Connect(config);
                        if (connection != null)
                        {
                            _redisConnectionList.Add(config, connection);
                        }
                    }

                    if (connection != null)
                    {
                        if (connection.State == RedisConnectionBase.ConnectionState.Closing
                            || connection.State == RedisConnectionBase.ConnectionState.Closed)
                        {
                            connection = ConnectionUtils.Connect(config);
                        }

                        if (connection.State == RedisConnectionBase.ConnectionState.New)
                        {
                            connection.Open();
                        }
                    }
                }
            }
            return connection;
        }

        public static string DefaultHostName
        {
            get
            {
                return Config.AppSettings("RedisCacheProvider.DefaultHostName", "localhost");
            }
        }

        public static int DefaultDatabase
        {
            get
            {
                return Config.AppSettings("RedisCacheProvider.DefaultDatabase", 1);
            }
        }

        #endregion

        private int _database;
        private string _hostName;
        private RedisConnection _connection;

        public RedisCacheProvider(CacheOptions options = null)
            : base(options)
        {
            _database = options != null ? options.Database : DefaultDatabase;
            _hostName = options != null ? options.HostName : DefaultHostName;
            _connection = GetRedisConnection(_hostName);
        }

        public override bool IsOutOfProcess
        {
            get
            {
                return true;
            }
        }

        public override void RemoveAll()
        {
            _connection.Server.FlushDb(_database);
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            var taskGet = _connection.Strings.Get(_database, key);
            var taskRemove = _connection.Keys.Remove(_database, key);
            return CacheValue.FromBytes(taskGet.Result);
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            var taskRemove = _connection.Keys.Remove(_database, key);
            return taskRemove.Result;
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var now = DateTimeOffset.Now;
            var data = ComputeValue((CacheValue)val, now);
            var taskAdd = _connection.Strings.SetIfNotExists(_database, key, data).ContinueWith(res =>
            {
                if (res.Result)
                {
                    SetExpiration(_connection, key, now);
                }
                return res.Result;
            });
            return taskAdd.Result;
        }

        public override bool Save(string key, object val)
        {
            using (var tran = _connection.CreateTransaction())
            {
                var now = DateTimeOffset.Now;
                SaveImplementation(tran, key, (CacheValue)val, now);
                return tran.Execute().Wait(_waitTimeOut);
            }
        }

        public override bool Save(IDictionary<string, object> items)
        {
            using (var tran = _connection.CreateTransaction())
            {
                var now = DateTimeOffset.Now;
                foreach (var item in items)
                {
                    SaveImplementation(tran, item.Key, (CacheValue)item.Value, now);
                }
                return tran.Execute().Wait(_waitTimeOut);
            }
        }

        private bool SaveImplementation(RedisTransaction tran, string key, CacheValue val, DateTimeOffset currentDateTime, long? version = null)
        {
            key = ComputeKey(key);

            if (version != null)
            {
                tran.AddCondition(Condition.KeyEquals(_database, "VERSION::" + key, version.Value));
            }

            var data = ComputeValue(val, currentDateTime);
            tran.Strings.Set(_database, key, data);
            SetExpiration(tran, key, currentDateTime);
            return true;
        }

        private void SetExpiration<T>(T conn, string key, DateTimeOffset currentDateTime)
            where T : RedisConnection
        {
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    conn.Keys.Expire(_database, key, (int)ExpiresAtSpecificTime.Value.Subtract(currentDateTime).TotalSeconds);
                    break;
                case CacheExpirationType.DateTime:
                    conn.Keys.Expire(_database, key, (int)ExpiresAt.Subtract(currentDateTime).TotalSeconds);
                    break;
                case CacheExpirationType.TimeSpan:
                    conn.Keys.Expire(_database, key, (int)LifeSpan.TotalSeconds);
                    break;
            }
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key);
            var taskGet = _connection.Strings.Get(_database, key);
            var buffer = taskGet.Result;
            return ProcessRetrieve(buffer, key);
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var keyMap = ComputeKey(keys);
            var keysArray = keyMap.Keys.ToArray();
            var realKeysArray = keyMap.Values.ToArray();
            var taskGet = _connection.Strings.Get(_database, keysArray);
            var data = taskGet.Result;

            var result = new Dictionary<string, object>();
            for(int i = 0; i < realKeysArray.Length; i++)
            {
                var buffer = data[i];
                if (buffer != null)
                {
                    var key = keysArray[i];
                    var originalKey = realKeysArray[i];

                    var item = ProcessRetrieve(buffer, key);
                    if (item != null)
                    {
                        result.Add(originalKey, item);
                    }
                }
            }
            return result;
        }

        private CacheValue ProcessRetrieve(byte[] result, string key)
        {
            var cacheValue = CacheValue.FromBytes(result);
            // If there is a revision mismatch simulate a miss
            if (cacheValue != null && !cacheValue.IsValidVersion(this.Signature))
            {
                cacheValue = null;
            }
            
            return cacheValue;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            key = ComputeKey(key);
            return _connection.Keys.Expire(_database, key, (int)lifeSpan.TotalSeconds).Wait(_waitTimeOut);
        }

        #region IRevisionProvider Members

        ulong IRevisionProvider.GetRevision(string key)
        {
            key = "REVISION::" + key;
            var task = _connection.Strings.GetInt64(_database, key);
            var result = task.Result;
            if (!result.HasValue)
            {
                var ticks = (ulong)GetTicks();
                var isSet = _connection.Strings.SetIfNotExists(_database, key, BitConverter.GetBytes(ticks));
                return isSet.Result ? ticks : ((IRevisionProvider)this).GetRevision(key);
            }
            else
            {
                return (ulong)result.Value;
            }
        }

        IDictionary<string, ulong> IRevisionProvider.GetRevisions(IEnumerable<string> keys)
        {
            var items = new Dictionary<string, ulong>();
            if (keys.Any())
            {
                var prefix = "REVISION::";
                var keyArray = keys.Select(k => prefix + k).ToArray();
                var task = _connection.Strings.Get(_database, keyArray);
                var values = task.Result;
                if (values != null)
                {
                    var missingKeys = new List<string>();

                    var result = new Dictionary<string, object>();
                    for (int i = 0; i < keyArray.Length; i++)
                    {
                        var buffer = values[i];
                        if (buffer != null)
                        {
                            items.Add(keyArray[i].Substring(prefix.Length), (ulong)BitConverter.ToInt64(buffer, 0));
                        }
                        else
                        {
                            missingKeys.Add(keyArray[i]);
                        }
                    }

                    foreach (var key in missingKeys)
                    {
                        var originalKey = key.Substring(prefix.Length);
                        var ticks = (ulong)GetTicks();
                        var isSet = _connection.Strings.SetIfNotExists(_database, key, BitConverter.GetBytes(ticks));
                        items.Add(originalKey, isSet.Result ? ticks : ((IRevisionProvider)this).GetRevision(originalKey));
                    }
                }
            }
            return items;
        }

        IDictionary<string, ulong> IRevisionProvider.GetAllRevisions()
        {
            var prefix = "REVISION::";
            var task = _connection.Keys.Find(_database, prefix + "*");
            var keys = task.Result;
            return ((IRevisionProvider)this).GetRevisions(keys);
        }

        ulong IRevisionProvider.IncrementRevision(string key, ulong delta = 1)
        {
            key = "REVISION::" + key;
            var task = _connection.Strings.Increment(_database, key, (long)delta);
            return (ulong)task.Result;
        }

        ulong IRevisionProvider.GenerateRevision()
        {
            return (ulong)GetTicks();
        }

        #endregion

        public override bool TryAcquireLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);

            var value = Guid.NewGuid().ToString();
            var stored = _connection.Strings.TakeLock(_database, key, value, ConfigurationFactory.Configuration.DistributedLockTimeout).Result;
            
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

        public override object WaitForItems(string key, int count = -1)
        {
            return null;
        }

        public override bool ReleaseLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);

            var removed = _connection.Strings.ReleaseLock(_database, key).Wait(_waitTimeOut);
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

        #region IPersistentCacheProvider Methods

        bool IPersistentCacheProvider.Append(string key, string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                using (var tran = _connection.CreateTransaction())
                {
                    key = ComputeKey(key);
                    tran.Strings.Append(_database, key, value);
                    return tran.Execute().Wait(_waitTimeOut);
                }
            }
            return false;
        }

        bool IPersistentCacheProvider.Save(string key, object value, object version)
        {
            using (var tran = _connection.CreateTransaction())
            {
                SaveImplementation(tran, key, (CacheValue)value, DateTimeOffset.Now, (long)version);
                return tran.Execute().Wait(_waitTimeOut);
            }
        }

        object IPersistentCacheProvider.Retrieve(string key, out object version)
        {
            key = ComputeKey(key);
            version = GenerateConcurrencyControlValue(key).Result;
            var taskGet = _connection.Strings.Get(_database, key);
            var buffer = taskGet.Result;
            return CacheValue.FromBytes(buffer);
        }

        IDictionary<string, object> IPersistentCacheProvider.Retrieve(IEnumerable<string> keys, out IDictionary<string, object> versions)
        {
            var keyMap = ComputeKey(keys);
            var keysArray = keyMap.Keys.ToArray();
            var realKeysArray = keyMap.Values.ToArray();
            var taskGet = _connection.Strings.Get(_database, keysArray);
            var data = taskGet.Result;

           var result = new Dictionary<string, object>();
            for (int i = 0; i < realKeysArray.Length; i++)
            {
                var buffer = data[i];
                if (buffer != null)
                {
                    var key = keysArray[i];
                    var originalKey = realKeysArray[i];

                    var item = ProcessRetrieve(buffer, key);
                    if (item != null)
                    {
                        result.Add(originalKey, item);
                    }
                }
            }

            versions = GenerateConcurrencyControlValues(keyMap).Result;

            return result;
        }

        private async Task<IDictionary<string, object>> GenerateConcurrencyControlValues(IDictionary<string, string> keys)
        {
            var items = new Dictionary<string, object>();

            var values = keys.Select(k => k.Value);

            var versionTasks = keys.Select(k => GenerateConcurrencyControlValue(k.Key));

            return values.Zip(await Task.WhenAll(versionTasks), (k, v) => new KeyValuePair<string, object>(k, v)).ToDictionary(k => k.Key, k => k.Value);
        }

        private async Task<long> GenerateConcurrencyControlValue(string prefixedKey)
        {
            var ticks = GetTicks();
            var version = ticks;
            using (var tran = _connection.CreateTransaction())
            {
                var versionKey = "CAS::" + prefixedKey;
                var setnx = await tran.Strings.SetIfNotExists(_database, versionKey, BitConverter.GetBytes(version));
                
                if (!setnx)
                {
                    var result = await tran.Strings.GetInt64(_database, versionKey);
                    if (result.HasValue)
                    {
                        version = result.Value;
                    }
                }

                if (!await tran.Execute())
                {
                    version = ticks;
                }
            }
            return ticks;
        }

        #endregion
    }
}
