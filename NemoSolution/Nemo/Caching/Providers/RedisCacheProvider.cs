using BookSleeve;
using Nemo.Configuration;
using Nemo.Extensions;
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

        public static RedisConnection GetRedisConnection(string hostName)
        {
            RedisConnection connection = null;
            hostName = !string.IsNullOrEmpty(hostName) ? hostName : DefaultHostName;
            if (hostName.NullIfEmpty() != null)
            {
                lock (_connectionLock)
                {
                    if (!_redisConnectionList.TryGetValue(hostName, out connection))
                    {
                        connection = new RedisConnection(hostName);
                        _redisConnectionList.Add(hostName, connection);
                    }

                    if (connection.State == RedisConnectionBase.ConnectionState.Closing 
                        || connection.State == RedisConnectionBase.ConnectionState.Closed)
                    {
                        connection = new RedisConnection(hostName);
                    }

                    if (connection.State == RedisConnectionBase.ConnectionState.New)
                    {
                        connection.Open();
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

        private bool SaveImplementation(RedisTransaction tran, string key, CacheValue val, DateTimeOffset currentDateTime)
        {
            key = ComputeKey(key);
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
            return ProcessRetrieve(buffer, key, this.ExpectedVersion);
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

                    var item = ProcessRetrieve(buffer, key, this.ExpectedVersion);
                    if (item != null)
                    {
                        result.Add(originalKey, item);
                    }
                }
            }
            return result;
        }

        private CacheValue ProcessRetrieve(byte[] result, string key, string expectedRevision)
        {
            var cacheValue = CacheValue.FromBytes(result);
            // If there is a revision mismatch simulate a miss
            if (cacheValue != null && !cacheValue.IsValidVersion(expectedRevision))
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

        public ulong GetRevision(string key)
        {
            key = "REVISION::" + key;
            var task = _connection.Strings.GetInt64(_database, key);
            var result = task.Result;
            if (!result.HasValue)
            {
                var isSet = _connection.Strings.SetIfNotExists(_database, key, BitConverter.GetBytes(1L));
                return isSet.Result ? 1ul : GetRevision(key);
            }
            else
            {
                return (ulong)result.Value;
            }
        }

        public IDictionary<string, ulong> GetRevisions(IEnumerable<string> keys)
        {
            var keyArray = keys.Select(k => "REVISION::" + k).ToArray();
            var task = _connection.Strings.Get(_database, keyArray);
            var values = task.Result;
            if (values != null)
            {
                var missingKeys = new List<string>();
                var items = new Dictionary<string, ulong>();

                var result = new Dictionary<string, object>();
                for (int i = 0; i < keyArray.Length; i++)
                {
                    var buffer = values[i];
                    if (buffer != null)
                    {
                        items.Add(keyArray[i], (ulong)BitConverter.ToInt64(buffer, 0));
                    }
                    else
                    {
                        missingKeys.Add(keyArray[i]);
                    }
                }
                
                foreach (var key in missingKeys)
                {
                    var isSet = _connection.Strings.SetIfNotExists(_database, key, BitConverter.GetBytes(1L));
                    items.Add(key, isSet.Result ? 1ul : GetRevision(key));
                }

                return items;
            }
            return null;
        }

        public ulong IncrementRevision(string key, ulong delta = 1)
        {
            key = "REVISION::" + key;
            var task = _connection.Strings.Increment(_database, key, (long)delta);
            return (ulong)task.Result;
        }

        public string ExpectedVersion { get; set; }
        
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

        public bool Append(string key, string value)
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

        public bool Save(string key, object value, object version)
        {
            using (var tran = _connection.CreateTransaction())
            {
                tran.AddCondition(Condition.KeyEquals(_database, "VERSION::" + key, (long?)version));
                SaveImplementation(tran, key, (CacheValue)value, DateTimeOffset.Now);
                return tran.Execute().Wait(_waitTimeOut);
            }
        }

        public object Retrieve(string key, out object version)
        {
            key = ComputeKey(key);
                
            using (var tran = _connection.CreateTransaction())
            {
                var versionKey = "VERSION::" + key;
                var ticks = DateTimeOffset.Now.Ticks;
                version = tran.Strings
                                    .SetIfNotExists(_database, versionKey, BitConverter.GetBytes(ticks))
                                    .ContinueWith(t => 
                                    {
                                        if (!t.Result)
                                        {
                                            return BitConverter.ToInt64(tran.Strings.Get(_database, versionKey).Result, 0);
                                        }
                                        else
                                        {
                                            return ticks;
                                        }
                                    }).Result;
                tran.Execute().Wait(_waitTimeOut);
            }

            var taskGet = _connection.Strings.Get(_database, key);
            var buffer = taskGet.Result;
            return CacheValue.FromBytes(buffer);
        }

        public IDictionary<string, object> Retrieve(IEnumerable<string> keys, out IDictionary<string, object> versions)
        {
            throw new NotImplementedException();
        }
    }
}
