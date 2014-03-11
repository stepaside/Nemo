using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.Utilities;
using System.Text;
using Nemo.Collections.Extensions;

namespace Nemo.Cache.Providers
{
    public class FileCacheProvider : CacheProvider, IPersistentCacheProvider
    {
        public const string CACHE_FILE_EXTENSION = ".cache";
        
        #region Static Declarations

        public static string DefaultFilePath
        {
            get
            {
                return Config.AppSettings("DiskCacheProvider.DefaultFilePath", Path.GetTempPath());
            }
        }

        private static object _diskCacheLock = new object();

        #endregion

        public FileCacheProvider(CacheOptions options = null)
            : base(options)
        {
            if (options != null && options.FilePath.NullIfEmpty() != null)
            {
                FilePath = options.FilePath;
            }
            else
            {
                FilePath = FileCacheProvider.DefaultFilePath;
            }
        }

        public string FilePath
        {
            get;
            private set;
        }

        public override bool IsOutOfProcess
        {
            get
            {
                return true;
            }
        }

        public override void Clear()
        {
            lock (_diskCacheLock)
            {
                var files = Directory.GetFiles(FilePath, "*" + CACHE_FILE_EXTENSION);
                if (files != null)
                {
                    for (int i = 0; i < files.Length; i++)
                    {
                        File.Delete(files[i]);
                    }
                }
            }
        }

        public override object Pop(string key)
        {
            lock (_diskCacheLock)
            {
                var result = Get(key);
                Remove(key);
                return result;
            }
        }

        public override bool Remove(string key)
        {
            lock (_diskCacheLock)
            {
                key = ComputeKey(key) + CACHE_FILE_EXTENSION;
                var file = Path.Combine(FilePath, key);
                var success = true;
                try
                {
                    File.Delete(file);
                }
                catch
                {
                    success = false;
                }
                return success;
            }
        }

        public override bool Add(string key, object val)
        {
            key = ComputeKey(key) + CACHE_FILE_EXTENSION;
            var file = Path.Combine(FilePath, key);
            var success = false;
            lock (_diskCacheLock)
            {
                if (!File.Exists(file) && val != null)
                {
                    Write(file, val);
                    success = true;
                }
            }
            return success;
        }

        public override bool Set(string key, object val)
        {
            var success = true;
            lock (_diskCacheLock)
            {
                if (!Directory.Exists(FilePath))
                {
                    Directory.CreateDirectory(FilePath);
                }

                key = ComputeKey(key) + CACHE_FILE_EXTENSION;
                success = SaveImplementation(key, val);
            }
            return success;
        }

        public override bool Set(IDictionary<string, object> items)
        {
            var success = true;
            lock (_diskCacheLock)
            {
                if (!Directory.Exists(FilePath))
                {
                    Directory.CreateDirectory(FilePath);
                }

                var keys = ComputeKey(items.Keys);
                foreach (var k in keys)
                {
                    success = success && SaveImplementation(k.Key + CACHE_FILE_EXTENSION, items[k.Value]);
                }
            }
            return success;
        }

        private bool SaveImplementation(string fileName, object val, bool append = false, DateTime? lastWriteTime = null)
        {
            var file = Path.Combine(FilePath, fileName);
            if (val != null)
            {
                if (lastWriteTime != null && File.GetLastWriteTime(file) >= lastWriteTime.Value)
                {
                    return false;
                }

                if (append)
                {
                    Append(file, val);
                }
                else
                {
                    Write(file, val);
                }
                return true;
            }
            return false;
        }

        public override object Get(string key)
        {
            key = ComputeKey(key) + CACHE_FILE_EXTENSION;
            DateTime? lastWriteTime;
            return RetrieveImplementation(key, out lastWriteTime);
        }

        public override IDictionary<string, object> Get(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            return computedKeys.ToDictionary(key => key.Value, key =>
            {
                DateTime? lastWriteTime;
                return RetrieveImplementation(key.Key + CACHE_FILE_EXTENSION, out lastWriteTime);
            });
        }

        private object RetrieveImplementation(string fileName, out DateTime? lastWriteTime)
        {
            lastWriteTime = null;
            var file = Path.Combine(FilePath, fileName);
            CacheValue result = null;
            if (File.Exists(file))
            {
                var fileInfo = new FileInfo(file);

                if ((ExpirationType == CacheExpirationType.Sliding && fileInfo.LastAccessTime.AddMilliseconds(LifeSpan.TotalMilliseconds) < DateTime.Now)
                    || (ExpirationType == CacheExpirationType.Absolute && fileInfo.LastAccessTime > ExpiresAt)
                    || (ExpirationType == CacheExpirationType.TimeOfDay && ExpiresAtSpecificTime.HasValue && fileInfo.LastAccessTime > ExpiresAtSpecificTime.Value))
                {
                    File.Delete(file);
                }
                else
                {
                    if (SlidingExpiration)
                    {
                        fileInfo.LastAccessTime = DateTime.Now;
                    }
                    result = Read(file);
                }

                lastWriteTime = fileInfo.LastWriteTime;
            }
            return result;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            return false;
        }

        protected CacheValue ExtractValue(object value)
        {
            return value is CacheItem ? ((CacheItem)value).Value : value as CacheValue;
        }

        private void Write(string file, object value)
        {
            var cacheValue = ExtractValue(value);
            File.WriteAllBytes(file, cacheValue.ToBytes());
        }

        private void Append(string file, object value)
        {
            File.AppendAllText(file, (string)value);
        }

        private CacheValue Read(string file)
        {
            var buffer = File.ReadAllBytes(file);
            return CacheValue.FromBytes(buffer);
        }

        #region IPersistentCacheProvider Methods

        bool IPersistentCacheProvider.Append(string key, string value)
        {
            var success = true;
            lock (_diskCacheLock)
            {
                if (!Directory.Exists(FilePath))
                {
                    Directory.CreateDirectory(FilePath);
                }

                key = ComputeKey(key) + CACHE_FILE_EXTENSION;
                success = SaveImplementation(key, value, true);
            }
            return success;
        }

        bool IPersistentCacheProvider.Set(string key, object value, object version)
        {
            var success = true;
            lock (_diskCacheLock)
            {
                if (!Directory.Exists(FilePath))
                {
                    Directory.CreateDirectory(FilePath);
                }

                key = ComputeKey(key) + CACHE_FILE_EXTENSION;
                success = SaveImplementation(key, value, false, (DateTime)version);
            }
            return success;
        }

        object IPersistentCacheProvider.Get(string key, out object version)
        {
            key = ComputeKey(key) + CACHE_FILE_EXTENSION;
            DateTime? lastWriteTime;
            var result = RetrieveImplementation(key, out lastWriteTime);
            version = lastWriteTime.Value; 
            return result;
        }

        IDictionary<string, object> IPersistentCacheProvider.Get(IEnumerable<string> keys, out IDictionary<string, object> versions)
        {
            var lastWriteTimeItems = new Dictionary<string, object>();
            var computedKeys = ComputeKey(keys);
            var result = computedKeys.ToDictionary(key => key.Value, key =>
            {
                DateTime? lastWriteTime;
                var item =  RetrieveImplementation(key.Key + CACHE_FILE_EXTENSION, out lastWriteTime);
                lastWriteTimeItems[key.Value] = lastWriteTime.Value;
                return item;
            });
            versions = lastWriteTimeItems;
            return result;
        }

        #endregion
    }
}
