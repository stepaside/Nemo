using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Nemo.Attributes;
using Nemo.Reflection;
using Nemo.Serialization;
using System.Transactions;
using Nemo.Data;
using System.Data.Common;
using System.Text;
using Nemo.Id;
using Nemo.Audit;
using Nemo.UnitOfWork;
using Nemo.Validation;
using Nemo.Security.Cryptography;

namespace Nemo.Extensions
{
    /// <summary>
    /// Extension methods for each DataEntity implementation to provide default ActiveRecord functionality.
    /// </summary>
    public static class ObjectExtensions
    {
        #region Property Accessor

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static TResult Property<T, TResult>(this T dataEntity, string propertyName)
            where T : class, IDataEntity
        {
            if (Reflector.IsMarkerInterface<T>())
            {
                return (TResult)Reflector.Property.Get(dataEntity.GetType(), dataEntity, propertyName);
            }
            else
            {
                return (TResult)Reflector.Property.Get<T>(dataEntity, propertyName);
            }
        }

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static TResult PropertyOrDefault<T, TResult>(this T dataEntity, string propertyName, TResult defaultValue)
            where T : class, IDataEntity
        {
            object result = null;
            if (Reflector.IsMarkerInterface<T>())
            {
                result = Reflector.Property.Get(dataEntity.GetType(), dataEntity, propertyName);
            }
            else
            {
                result = Reflector.Property.Get<T>(dataEntity, propertyName);
            }
            return result != null ? (TResult)result : defaultValue;
        }

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static object Property<T>(this T dataEntity, string propertyName)
            where T : class, IDataEntity
        {
            return dataEntity.Property<T, object>(propertyName);
        }

        /// <summary>
        /// Property method sets a value of a property.
        /// </summary>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        /// <param name="propertyValue"></param>
        public static void Property<T>(this T dataEntity, string propertyName, object propertyValue)
            where T : class, IDataEntity
        {
            if (Reflector.IsMarkerInterface<T>())
            {
                Reflector.Property.Set(dataEntity.GetType(), dataEntity, propertyName, propertyValue);
            }
            else
            {
                Reflector.Property.Set<T>(dataEntity, propertyName, propertyValue);
            }
        }

        /// <summary>
        /// PropertyExists method verifies if the property has value.
        /// </summary>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        public static bool PropertyExists<T>(this T dataEntity, string propertyName)
            where T : class, IDataEntity
        {
            object value;
            return dataEntity.PropertyTryGet(propertyName, out value);
        }

        /// <summary>
        /// PropertyTryGet method verifies if the property has value and returns the value.
        /// </summary>
        /// <param name="dataEntity"></param>
        /// <param name="propertyName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool PropertyTryGet<T>(this T dataEntity, string propertyName, out object value)
            where T : class, IDataEntity
        {
            var exists = false;
            value = null;
            try
            {
                value = dataEntity.Property(propertyName);
                exists = true;
            }
            catch { }
            return exists;
        }

        #endregion

        #region CRUD Methods

        /// <summary>
        /// Populate method provides an ability to populate an object by primary key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        public static void Populate<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            dataEntity.ThrowIfNull("dataEntity");
            dataEntity.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            // Convert readable primary key properties to rule parameters
            var parameters = propertyMap.Values
                            .Where(p => p.CanRead && p.IsPrimaryKey)
                            .Select(p => new Param
                            {
                                Name = p.ParameterName ?? p.PropertyName,
                                Value = dataEntity.Property<T>(p.PropertyName),
                                Direction = ParameterDirection.Input
                            }).ToArray();

            T retrievedObject = ObjectFactory.Retrieve<T>(parameters: parameters).FirstOrDefault();

            if (retrievedObject != null)
            {
                ObjectFactory.Map(retrievedObject, dataEntity, true);
                if (dataEntity is ITrackableDataEntity)
                {
                    ((ITrackableDataEntity)dataEntity).ObjectState = ObjectState.Clean;
                }
            }
        }

        /// <summary>
        /// Insert method provides an ability to insert an object to the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <param name="additionalParameters"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public static bool Insert<T>(this T dataEntity, params Param[] additionalParameters)
            where T : class, IDataEntity
        {
            dataEntity.ThrowIfNull("dataEntity");
            dataEntity.CheckReadOnly();

            // Validate an object before persisting
            var errors = dataEntity.Validate();
            if (errors.Any())
            {
                throw new ValidationException(errors);
            }

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            var identityProperty = propertyMap
                                        .Where(p => p.Key.CanWrite && p.Value != null && p.Value.IsAutoGenerated)
                                        .Select(p => p.Key)
                                        .FirstOrDefault();
            var outputProperties = propertyMap
                                        .Where(p => p.Key.CanWrite && p.Value != null
                                                    && (p.Value.Direction == ParameterDirection.InputOutput
                                                        || p.Value.Direction == ParameterDirection.Output))
                                        .Select(p => p.Key);

            // Generate key if primary key value was not set and no identity (autogenerated) property is defined
            if (identityProperty == null && dataEntity.IsNew())
            {
                dataEntity.GenerateKey();
            }

            string identityPropertyName = null;
            var parameters = GetInsertParameters(dataEntity, propertyMap);

            if (additionalParameters != null && additionalParameters.Length > 0)
            {
                var tempParameters = additionalParameters.GroupJoin(parameters, p => p.Name, p => p.Name, (a, p) => p.Any() ? p.First() : a).ToArray();
                parameters = tempParameters;
            }

            var response = ObjectFactory.Insert<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                if (identityProperty != null)
                {
                    object identityValue = parameters.Single(p => p.Name == identityPropertyName).Value;
                    if (identityValue != null && !Convert.IsDBNull(identityValue))
                    {
                        Reflector.Property.Set<T>(dataEntity, identityProperty.Name, identityValue);
                    }
                }

                Identity.Get<T>().Set(dataEntity);

                SetOutputParameterValues<T>(dataEntity, outputProperties, propertyMap, parameters);

                if (dataEntity is ITrackableDataEntity)
                {
                    ((ITrackableDataEntity)dataEntity).ObjectState = ObjectState.Clean;
                }

                if (dataEntity is IAuditable)
                {
                    var logProvider = AuditLogProvider.Current;
                    if (logProvider != null)
                    {
                        logProvider.Write<T>(new AuditLog<T>(ObjectFactory.OPERATION_INSERT, default(T), dataEntity));
                    }
                }
            }

            return success;
        }

        /// <summary>
        ///  Update method provides an ability to update an object in the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static bool Update<T>(this T dataEntity, params Param[] additionalParameters)
            where T : class, IDataEntity
        {
            dataEntity.ThrowIfNull("dataEntity");
            dataEntity.CheckReadOnly();

            // Validate an object before persisting
            var errors = dataEntity.Validate();
            if (errors.Any())
            {
                throw new ValidationException(errors);
            }

            var errorMessage = string.Empty;
            var supportsChangeTracking = dataEntity is ITrackableDataEntity;
            if (supportsChangeTracking && ((ITrackableDataEntity)dataEntity).IsReadOnly())
            {
                throw new ApplicationException("Update Failed: provided object is read-only.");
            }

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();
            var outputProperties = propertyMap.Keys
                                        .Where(p => p.CanWrite && propertyMap[p] != null
                                                    && (propertyMap[p].Direction == ParameterDirection.InputOutput
                                                        || propertyMap[p].Direction == ParameterDirection.Output));

            var parameters = GetUpdateParameters(dataEntity, propertyMap);

            if (additionalParameters != null && additionalParameters.Length > 0)
            {
                var tempParameters = additionalParameters.GroupJoin(parameters, p => p.Name, p => p.Name, (a, p) => p.Any() ? p.First() : a).ToArray();
                parameters = tempParameters;
            }

            var response = ObjectFactory.Update<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                Identity.Get<T>().Set(dataEntity);

                SetOutputParameterValues<T>(dataEntity, outputProperties, propertyMap, parameters);

                if (supportsChangeTracking)
                {
                    ((ITrackableDataEntity)dataEntity).ObjectState = ObjectState.Clean;
                }

                if (dataEntity is IAuditable)
                {
                    var logProvider = AuditLogProvider.Current;
                    if (logProvider != null)
                    {
                        logProvider.Write<T>(new AuditLog<T>(ObjectFactory.OPERATION_UPDATE, (dataEntity.Old() ?? dataEntity), dataEntity));
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Delete method provides an ability to soft-delete an object from the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <returns></returns>
        public static bool Delete<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            dataEntity.ThrowIfNull("dataEntity");
            dataEntity.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            var parameters = GetDeleteParameters(dataEntity, propertyMap);

            var response = ObjectFactory.Delete<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                Identity.Get<T>().Remove(dataEntity);

                if (dataEntity is ITrackableDataEntity)
                {
                    ((ITrackableDataEntity)dataEntity).ObjectState = ObjectState.Deleted;
                }

                if (dataEntity is IAuditable)
                {
                    var logProvider = AuditLogProvider.Current;
                    if (logProvider != null)
                    {
                        logProvider.Write<T>(new AuditLog<T>(ObjectFactory.OPERATION_DELETE, dataEntity, default(T)));
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Destroy method provides an ability to hard-delete an object from the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <returns></returns>
        public static bool Destroy<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            dataEntity.ThrowIfNull("dataEntity");
            dataEntity.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            var parameters = GetDeleteParameters(dataEntity, propertyMap);

            var response = ObjectFactory.Destroy<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                Identity.Get<T>().Remove(dataEntity);

                if (dataEntity is ITrackableDataEntity)
                {
                    ((ITrackableDataEntity)dataEntity).ObjectState = ObjectState.Deleted;
                }

                if (dataEntity is IAuditable)
                {
                    var logProvider = AuditLogProvider.Current;
                    if (logProvider != null)
                    {
                        logProvider.Write<T>(new AuditLog<T>(ObjectFactory.OPERATION_DESTROY, dataEntity, default(T)));
                    }
                }
            }

            return success;
        }

        #region ITrackableDataEntity Methods

        public static bool Save<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            var result = false;
            
            if (dataEntity.IsNew())
            {
                result = dataEntity.Insert();
            }
            else if (!(dataEntity is ITrackableDataEntity) || ((ITrackableDataEntity)dataEntity).IsDirty())
            {
                result = dataEntity.Update();
            }

            return result;
        }

        public static bool IsNew<T>(this T dataEntity)
          where T : class, IDataEntity
        {
            if (dataEntity is ITrackableDataEntity)
            {
                return ((ITrackableDataEntity)dataEntity).ObjectState == ObjectState.New;
            }
            else
            {
                var primaryKey = dataEntity.GetPrimaryKey();
                return primaryKey.Values.Sum(v => v == null || v == v.GetType().GetDefault() ? 1 : 0) == primaryKey.Values.Count;
            }
        }

        public static bool IsReadOnly<T>(this T dataEntity)
           where T : class, IDataEntity
        {
            if (dataEntity is ITrackableDataEntity)
            {
                return ((ITrackableDataEntity)dataEntity).ObjectState == ObjectState.ReadOnly;
            }
            return Reflector.GetAttribute<ReadOnlyAttribute>(dataEntity.GetType(), false) != null;
        }

        public static bool IsDirty<T>(this T dataEntity)
           where T : class, ITrackableDataEntity
        {
            return dataEntity.ObjectState == ObjectState.Dirty;
        }

        public static bool IsDeleted<T>(this T dataEntity)
           where T : class, ITrackableDataEntity
        {
            return dataEntity.ObjectState == ObjectState.Deleted;
        }

        #endregion

        #region Parameter Methods

        internal static Param[] GetInsertParameters(object dataEntity, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                            .Where(p => p.IsPersistent && (p.IsSimpleType || p.IsSimpleList) && (p.CanWrite || p.IsAutoGenerated))
                            .Select(p => new Param
                            {
                                Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                                Value = GetParameterValue(dataEntity, p),
                                DbType = Reflector.ClrToDbType(p.PropertyType),
                                Direction = p.IsAutoGenerated ? ParameterDirection.Output : p.Direction,
                                Source = p.MappedColumnName,
                                IsAutoGenerated = p.IsAutoGenerated,
                                IsPrimaryKey = p.IsPrimaryKey
                            });
            return parameters.ToArray();
        }

        internal static Param[] GetUpdateParameters(object dataEntity, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                    .Where(p => p.IsPersistent && (p.IsSimpleType || p.IsSimpleList) && (p.CanWrite || p.IsAutoGenerated))
                    .Select(p => new Param
                    {
                        Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                        Value = GetParameterValue(dataEntity, p),
                        DbType = Reflector.ClrToDbType(p.PropertyType),
                        Direction = p.Direction,
                        Source = p.MappedColumnName,
                        IsAutoGenerated = p.IsAutoGenerated,
                        IsPrimaryKey = p.IsPrimaryKey
                    });

            return parameters.ToArray();
        }

        internal static Param[] GetDeleteParameters(object dataEntity, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                    .Where(p => p.CanRead && p.IsPrimaryKey)
                    .Select(p => new Param
                    {
                        Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                        Value = GetParameterValue(dataEntity, p),
                        Source = p.MappedColumnName,
                        IsPrimaryKey = true
                    });

            return parameters.ToArray();
        }

        private static object GetParameterValue(object dataEntity, ReflectedProperty property)
        {
            var result = Reflector.Property.Get(dataEntity.GetType(), dataEntity, property.PropertyName);
            if (result != null && property.IsSimpleList && property.ElementType != typeof(byte))
            {
                result = ((IEnumerable)result).SafeCast<string>().ToDelimitedString(",");
            }
            return result;
        }

        private static void SetOutputParameterValues<T>(T dataEntity, IEnumerable<PropertyInfo> outputProperties, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, IList<Param> parameters)
        {
            var parameterMap = parameters.GroupBy(p => p.Name).ToDictionary(g => g.Key, g => g.First().Value);

            // Set output parameter values
            foreach (var outputProperty in outputProperties)
            {
                string outputPropertyName = null;
                if (propertyMap[outputProperty] != null && !string.IsNullOrEmpty(propertyMap[outputProperty].ParameterName))
                {
                    outputPropertyName = propertyMap[outputProperty].ParameterName;
                }
                else
                {
                    outputPropertyName = outputProperty.Name;
                }

                object outputPropertyValue;
                if (parameterMap.TryGetValue(outputPropertyName, out outputPropertyValue) && !Convert.IsDBNull(outputPropertyValue))
                {
                    Reflector.Property.Set<T>(dataEntity, outputProperty.Name, outputPropertyValue);
                }
            }
        }

        #endregion

        #endregion

        #region Hash/ID Generation Methods

        private static ConcurrentDictionary<Type, string[]> _primaryKeys = new ConcurrentDictionary<Type, string[]>();
        private static ConcurrentDictionary<Type, string[]> _primaryAndCacheKeys = new ConcurrentDictionary<Type, string[]>();
        private static ConcurrentDictionary<Tuple<Type, PropertyInfo, Type>, IIdGenerator> _idGenerators = new ConcurrentDictionary<Tuple<Type, PropertyInfo, Type>, IIdGenerator>();
        
        /// <summary>
        /// GetPrimaryKey method returns primary key of a business object (if available)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataEntity"></param>
        /// <param name="includeCacheKey"></param>
        /// <returns></returns>
        public static IDictionary<string, object> GetPrimaryKey<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            // Get properties and build a property map
            var interfaceType = typeof(T);
            if (Reflector.IsMarkerInterface<T>())
            {
                interfaceType = dataEntity.GetType();
            }

            string[] primaryKeyProperties = null;
            if (interfaceType != null)
            {
                primaryKeyProperties = _primaryAndCacheKeys.GetOrAdd(interfaceType, type => ObjectFactory.GetPrimaryKeyProperties(type));
            }
            else
            {
                primaryKeyProperties = new string[] { };
            }

            var primaryKey = new SortedDictionary<string, object>();

            for (int i = 0; i < primaryKeyProperties.Length; i++)
            {
                var value = dataEntity.Property(primaryKeyProperties[i]);
                primaryKey[primaryKeyProperties[i]] = value;
            }

            return primaryKey;
        }

        public static void GenerateKey<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            var propertyMap = Reflector.GetPropertyMap<T>();
            var generatorKeys = propertyMap.Where(p => p.Value != null && p.Value.Generator != null).Select(p => Tuple.Create(typeof(T), p.Key, p.Value.Generator));
            foreach (var key in generatorKeys)
            {
                var generator = _idGenerators.GetOrAdd(key, k => (IIdGenerator)Nemo.Reflection.Activator.New(k.Item3));

                dataEntity.Property(key.Item2.Name, generator.Generate());
            }
        }

        public static string ComputeHash<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            var hash = Jenkins96Hash.Compute(Encoding.UTF8.GetBytes(string.Join(",", dataEntity.GetPrimaryKey<T>().Select(p => string.Format("{0}={1}", p.Key, p.Value)))));
            return typeof(T).FullName + "/" + hash;
        }

        #endregion
        
        #region ReadOnly Methods

        public static T AsReadOnly<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            if (dataEntity == null)
            {
                return null;
            }
            return Adapter.Guard(dataEntity);
        }

        public static List<T> AsReadOnly<T>(this List<T> dataEntitys)
            where T : class, IDataEntity
        {
            if (dataEntitys == null)
            {
                return null;
            }
            return dataEntitys.Select(b => b.AsReadOnly()).ToList();
        }

        public static IList<T> AsReadOnly<T>(this IList<T> dataEntitys)
            where T : class, IDataEntity
        {
            if (dataEntitys == null)
            {
                return null;
            }
            return dataEntitys.Select(b => b.AsReadOnly()).ToArray();
        }

        internal static void CheckReadOnly<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            // Read-only objects can't participate in CRUD
            if (dataEntity.IsReadOnly())
            {
                throw new NotSupportedException("Operation is not allowed: object instance is read-only.");
            }
        }

        #endregion

        #region Clone Methods

        /// <summary>
        /// Creates a deep copy of the interface instance. 
        /// NOTE: The object must be serializable.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance"></param>
        /// <returns></returns>
        public static T Clone<T>(this T instance)
            where T : class, IDataEntity
        {
            var data = instance.Serialize(SerializationMode.SerializeAll);
            var value = ObjectSerializer.Deserialize<T>(data);
            return value;
        }

        /// <summary>
        /// Creates a deep copy of the collection of interface instances. 
        /// NOTE: The object must be serializable.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance"></param>
        /// <returns></returns>
        public static IEnumerable<T> Clone<T>(this IEnumerable<T> collection)
            where T : class, IDataEntity
        {
            var data = collection.Serialize(SerializationMode.SerializeAll);
            var value = ObjectSerializer.Deserialize<T>(data);
            return value;
        }

        #endregion
    }
}
