using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Nemo.Extensions;
using Nemo.Serialization;
using Nemo.Utilities;
using System.Text;
using Nemo.Collections.Extensions;

namespace Nemo.Caching.Providers
{
    public class FileCacheProvider : CacheProvider
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
            : base(CacheType.File, options)
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

        public override void RemoveAll()
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

        public override object Remove(string key)
        {
            lock (_diskCacheLock)
            {
                var result = Retrieve(key);
                Clear(key);
                return result;
            }
        }

        public override bool Clear(string key)
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

        public override bool AddNew(string key, object val)
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

        public override bool Save(string key, object val)
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

        public override bool Save(IDictionary<string, object> items)
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

        private bool SaveImplementation(string fileName, object val)
        {
            var file = Path.Combine(FilePath, fileName);
            if (val != null)
            {
                Write(file, val);
                return true;
            }
            return false;
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key) + CACHE_FILE_EXTENSION;
            return RetrieveImplementation(key);
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            return computedKeys.ToDictionary(key => key.Value, key => RetrieveImplementation(key.Key + CACHE_FILE_EXTENSION));
        }

        private object RetrieveImplementation(string fileName)
        {
            var file = Path.Combine(FilePath, fileName);
            object result = null;
            if (File.Exists(file))
            {
                var fileInfo = new FileInfo(file);

                if ((ExpirationType == CacheExpirationType.TimeSpan && fileInfo.LastAccessTime.AddMilliseconds(LifeSpan.TotalMilliseconds) < DateTime.Now)
                    || (ExpirationType == CacheExpirationType.DateTime && fileInfo.LastAccessTime > ExpiresAt)
                    || (ExpirationType == CacheExpirationType.TimeOfDay && (DateTimeOffset)fileInfo.LastAccessTime > ExpiresAtSpecificTime))
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
            }
            return result;
        }

        private void Write(string file, object value)
        {
            var buffer = SerializationWriter.WriteObjectWithType(value);
            File.WriteAllBytes(file, buffer);
        }

        private object Read(string file)
        {
            var buffer = File.ReadAllBytes(file);
            var result = SerializationReader.ReadObjectWithType(buffer);
            return result;
        }
    }
}
