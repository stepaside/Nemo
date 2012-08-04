using System;
using System.Collections.Generic;
using System.Linq;
using BookSleeve;
using Nemo.Serialization;
using Nemo.Utilities;
using System.Collections.Concurrent;
using Nemo.Extensions;

namespace Nemo.Caching.Providers
{
    public class RedisCacheProvider : CacheProvider
    {
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

        public RedisCacheProvider(int database, string hostName)
            : base(CacheType.Redis, null)
        {
            _database = database;
            _hostName = hostName;
            _connection = GetRedisConnection(_hostName);
        }

        public RedisCacheProvider(CacheOptions options = null)
            : base(CacheType.Redis, options)
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
            var taskGet = _connection.Strings.Get(_database, key);
            var taskRemove = _connection.Keys.Remove(_database, key);
            taskGet.Wait();
            return taskGet.Result;
        }

        public override bool Clear(string key)
        {
            var taskRemove = _connection.Keys.Remove(_database, key);
            taskRemove.Wait();
            return taskRemove.Result;
        }

        public override bool AddNew(string key, object val)
        {
            var data = SerializationWriter.WriteObjectWithType(val);
            var taskAdd = _connection.Strings.SetIfNotExists(_database, key, data);
            taskAdd.Wait();
            return taskAdd.Result;
        }

        public override bool Save(string key, object val)
        {
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
                values.Add(item.Key, data);
            }

            var taskAdd = _connection.Strings.Set(_database, values);
            taskAdd.Wait();
            return true;
        }

        public override object Retrieve(string key)
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
            var keysArray = keys.ToArray();
            var taskGet = _connection.Strings.Get(_database, keysArray);
            taskGet.Wait();
                
            var result = new Dictionary<string, object>();
            for (int i = 0; i < keysArray.Length; i++)
            {
                var buffer = taskGet.Result[i];
                if (buffer != null)
                {
                    var item = SerializationReader.ReadObjectWithType(buffer);                    
                    result[keysArray[i]] = item;
                }
            }
            return result;
        }
    }
}
