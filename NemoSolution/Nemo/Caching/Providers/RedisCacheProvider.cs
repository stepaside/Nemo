using System;
using System.Collections.Generic;
using System.Linq;
using BookSleeve;
using Nemo.Serialization;
using Nemo.Utilities;
using System.Collections.Concurrent;
using Nemo.Extensions;
using System.Threading;

namespace Nemo.Caching.Providers
{
    public class RedisCacheProvider : DistributedCacheProviderWithLockManager<RedisCacheProvider>, IDistributedCounter, IPersistentCacheProvider
    {
        private readonly int _distributedLockRetryCount = ObjectFactory.Configuration.DistributedLockRetryCount;
        private readonly double _distributedLockWaitTime = ObjectFactory.Configuration.DistributedLockWaitTime;
        
        #region Static Declarations

        private static Dictionary<string, RedisConnection> _redisConnectionList = new Dictionary<string, RedisConnection>();

        private static object _connectionLock = new object();

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

                    if (connection.State == RedisConnectionBase.ConnectionState.Shiny)
                    {
                        var openAsync = connection.Open();
                        connection.Wait(openAsync);
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

        internal RedisCacheProvider(int database, string hostName)
            : base(null, null)
        {
            _database = database;
            _hostName = hostName;
            _connection = GetRedisConnection(_hostName);
        }

        public RedisCacheProvider(CacheOptions options = null)
            : base(new RedisCacheProvider(options != null ? options.Database : DefaultDatabase, options != null ? options.HostName : DefaultHostName), options)
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
            _connection.Server.FlushAll();
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            var taskGet = _connection.Strings.Get(_database, key);
            var taskRemove = _connection.Keys.Remove(_database, key);
            taskGet.Wait();
            return taskGet.Result;
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            var taskRemove = _connection.Keys.Remove(_database, key);
            taskRemove.Wait();
            return taskRemove.Result;
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var data = SerializationWriter.WriteObjectWithType(val);
            var taskAdd = _connection.Strings.SetIfNotExists(_database, key, data);
            taskAdd.Wait();
            return taskAdd.Result;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            var data = SerializationWriter.WriteObjectWithType(val);
            var taskAdd = _connection.Strings.Set(_database, key, data);
            taskAdd.Wait();
            return true;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var values = new Dictionary<string, byte[]>();
            foreach (var item in items)
            {
                var data = SerializationWriter.WriteObjectWithType(item.Value);
                values.Add(ComputeKey(item.Key), data);
            }

            var taskAdd = _connection.Strings.Set(_database, values);
            taskAdd.Wait();
            return true;
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key);
            return RetrieveUsingRawKey(key);
        }

        public override object RetrieveUsingRawKey(string key)
        {
            var taskGet = _connection.Strings.Get(_database, key);
            taskGet.Wait();
            var buffer = taskGet.Result;
            if (buffer != null)
            {
                var result = SerializationReader.ReadObjectWithType(buffer);
                return result;
            }
            return null;
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var keyMap = ComputeKey(keys);
            var keysArray = keyMap.Keys.ToArray();
            var realKeysArray = keyMap.Values.ToArray();
            var taskGet = _connection.Strings.Get(_database, keysArray);
            taskGet.Wait();
                
            var result = new Dictionary<string, object>();
            for (int i = 0; i < realKeysArray.Length; i++)
            {
                var buffer = taskGet.Result[i];
                if (buffer != null)
                {
                    var item = SerializationReader.ReadObjectWithType(buffer);
                    result[realKeysArray[i]] = item;
                }
            }
            return result;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            return false;
        }

        #region IDistributedCounter Members

        public ulong Increment(string key, ulong delta = 1)
        {
            key = ComputeKey(key);
            var task = _connection.Strings.Increment(_database, key, (long)delta);
            task.Wait();
            return (ulong)task.Result;
        }

        public ulong Decrement(string key, ulong delta = 1)
        {
            key = ComputeKey(key);
            var task = _connection.Strings.Decrement(_database, key, (long)delta);
            task.Wait();
            return (ulong)task.Result;
        }

        #endregion
    }
}
