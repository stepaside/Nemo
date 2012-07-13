using System;
using System.Collections.Generic;
using System.Linq;
using BookSleeve;
using Nemo.Serialization;
using Nemo.Utilities;

namespace Nemo.Caching.Providers
{
    public class RedisCacheProvider : CacheProvider, IDisposable
    {
        #region Static Declarations

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
            _connection = new RedisConnection(_hostName);
            _connection.Open();
        }

        public RedisCacheProvider(CacheOptions options = null)
            : base(CacheType.Redis, options)
        {
            _database = options != null ? options.Database : DefaultDatabase;
            _hostName = options != null ? options.HostName : DefaultHostName;
            _connection = new RedisConnection(_hostName);
            _connection.Open();
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
            var writer = SerializationWriter.CreateWriter(false);
            writer.WriteObject(val);
            var data = writer.GetBytes();

            var taskAdd = _connection.Strings.SetIfNotExists(_database, key, data);
            taskAdd.Wait();
            return taskAdd.Result;
        }

        public override bool Save(string key, object val)
        {
            var writer = SerializationWriter.CreateWriter(false);
            writer.WriteObject(val);
            var data = writer.GetBytes();

            var taskAdd = _connection.Strings.Set(_database, key, data);
            taskAdd.Wait();
            return true;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var values = new Dictionary<string, byte[]>();
            foreach (var item in items)
            {
                var writer = SerializationWriter.CreateWriter(false);
                writer.WriteObject(item.Value);
                var data = writer.GetBytes();
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
            var reader = SerializationReader.CreateReader(taskGet.Result);
            var result = reader.ReadObject();
            return result;
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var keysArray = keys.ToArray();
            var taskGet = _connection.Strings.Get(_database, keysArray);
            taskGet.Wait();
                
            var result = new Dictionary<string, object>();
            for (int i = 0; i < keysArray.Length; i++)
            {
                var reader = SerializationReader.CreateReader(taskGet.Result[i]);
                var item = reader.ReadObject();
                result[keysArray[i]] = item;
            }
            return result;
        }

        void IDisposable.Dispose()
        {
            _connection.Close(false);
        }
    }
}
