using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Nemo.Attributes;
using Nemo.Caching;
using Nemo.Reflection;
using Nemo.Validation;
using Nemo.Serialization;
using System.Transactions;
using Nemo.Data;
using System.Data.Common;
using System.Text;
using Nemo.Id;

namespace Nemo.Extensions
{
    /// <summary>
    /// Extension methods for each BusinessObject implementation to provide default ActiveRecord functionality.
    /// </summary>
    public static class ObjectExtensions
    {
        #region Property Accessor

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static TResult Property<T, TResult>(this T businessObject, string propertyName)
            where T : class, IBusinessObject
        {
            if (Reflector.IsMarkerInterface<T>())
            {
                return (TResult)Reflector.Property.Get(businessObject.GetType(), businessObject, propertyName);
            }
            else
            {
                return (TResult)Reflector.Property.Get<T>(businessObject, propertyName);
            }
        }

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static TResult PropertyOrDefault<T, TResult>(this T businessObject, string propertyName, TResult defaultValue)
            where T : class, IBusinessObject
        {
            object result = null;
            if (Reflector.IsMarkerInterface<T>())
            {
                result = Reflector.Property.Get(businessObject.GetType(), businessObject, propertyName);
            }
            else
            {
                result = Reflector.Property.Get<T>(businessObject, propertyName);
            }
            return result != null ? (TResult)result : defaultValue;
        }

        /// <summary>
        /// Property method returns a value of a property.
        /// </summary>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static object Property<T>(this T businessObject, string propertyName)
            where T : class, IBusinessObject
        {
            return businessObject.Property<T, object>(propertyName);
        }

        /// <summary>
        /// Property method sets a value of a property.
        /// </summary>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        /// <param name="propertyValue"></param>
        public static void Property<T>(this T businessObject, string propertyName, object propertyValue)
            where T : class, IBusinessObject
        {
            if (Reflector.IsMarkerInterface<T>())
            {
                Reflector.Property.Set(businessObject.GetType(), businessObject, propertyName, propertyValue);
            }
            else
            {
                Reflector.Property.Set<T>(businessObject, propertyName, propertyValue);
            }
        }

        /// <summary>
        /// PropertyExists method verifies if the property has value.
        /// </summary>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        public static bool PropertyExists<T>(this T businessObject, string propertyName)
            where T : class, IBusinessObject
        {
            object value;
            return businessObject.PropertyTryGet(propertyName, out value);
        }

        /// <summary>
        /// PropertyTryGet method verifies if the property has value and returns the value.
        /// </summary>
        /// <param name="businessObject"></param>
        /// <param name="propertyName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool PropertyTryGet<T>(this T businessObject, string propertyName, out object value)
            where T : class, IBusinessObject
        {
            var exists = false;
            value = null;
            try
            {
                value = businessObject.Property(propertyName);
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
        /// <param name="businessObject"></param>
        public static void Populate<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            businessObject.ThrowIfNull("businessObject");
            businessObject.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            // Convert readable primary key properties to rule parameters
            var parameters = propertyMap.Values
                            .Where(p => p.CanRead && p.IsPrimaryKey)
                            .Select(p => new Param
                            {
                                Name = p.ParameterName ?? p.PropertyName,
                                Value = businessObject.Property<T>(p.PropertyName),
                                Direction = ParameterDirection.Input
                            }).ToArray();

            T retrievedObject = ObjectFactory.Retrieve<T>(parameters: parameters).FirstOrDefault();

            if (retrievedObject != null)
            {
                ObjectFactory.Map(retrievedObject, businessObject, true);
                if (businessObject is IChangeTrackingBusinessObject)
                {
                    ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Clean;
                }
            }
        }

        /// <summary>
        /// Insert method provides an ability to insert an object to the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <param name="additionalParameters"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public static bool Insert<T>(this T businessObject, params Param[] additionalParameters)
            where T : class, IBusinessObject
        {
            businessObject.ThrowIfNull("businessObject");
            businessObject.CheckReadOnly();

            // Validate an object before persisting
            var errors = businessObject.Validate();
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
            if (identityProperty == null && businessObject.IsNew())
            {
                businessObject.GenerateKey();
            }

            string identityPropertyName = null;
            var parameters = GetInsertParameters(businessObject, propertyMap);

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
                        Reflector.Property.Set<T>(businessObject, identityProperty.Name, identityValue);
                    }
                }

                ObjectCache.RemoveLinks(businessObject);

                SetOutputParameterValues<T>(businessObject, outputProperties, propertyMap, parameters);

                if (businessObject is IChangeTrackingBusinessObject)
                {
                    ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Clean;
                }
            }

            return success;
        }

        /// <summary>
        ///  Update method provides an ability to update an object in the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static bool Update<T>(this T businessObject, params Param[] additionalParameters)
            where T : class, IBusinessObject
        {
            businessObject.ThrowIfNull("businessObject");
            businessObject.CheckReadOnly();

            // Validate an object before persisting
            var errors = businessObject.Validate();
            if (errors.Any())
            {
                throw new ValidationException(errors);
            }

            var errorMessage = string.Empty;
            var supportsChangeTracking = businessObject is IChangeTrackingBusinessObject;
            if (supportsChangeTracking && ((IChangeTrackingBusinessObject)businessObject).IsReadOnly())
            {
                throw new ApplicationException("Update Failed: provided object is read-only.");
            }

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();
            var outputProperties = propertyMap.Keys
                                        .Where(p => p.CanWrite && propertyMap[p] != null
                                                    && (propertyMap[p].Direction == ParameterDirection.InputOutput
                                                        || propertyMap[p].Direction == ParameterDirection.Output));

            var parameters = GetUpdateParameters(businessObject, propertyMap);

            if (additionalParameters != null && additionalParameters.Length > 0)
            {
                var tempParameters = additionalParameters.GroupJoin(parameters, p => p.Name, p => p.Name, (a, p) => p.Any() ? p.First() : a).ToArray();
                parameters = tempParameters;
            }

            var response = ObjectFactory.Update<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                ObjectCache.Remove(businessObject);

                SetOutputParameterValues<T>(businessObject, outputProperties, propertyMap, parameters);

                if (supportsChangeTracking)
                {
                    ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Clean;
                }
            }

            return success;
        }

        /// <summary>
        /// Delete method provides an ability to soft-delete an object from the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <returns></returns>
        public static bool Delete<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            businessObject.ThrowIfNull("businessObject");
            businessObject.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            var parameters = GetDeleteParameters(businessObject, propertyMap);

            var response = ObjectFactory.Delete<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                ObjectCache.Remove(businessObject);

                if (businessObject is IChangeTrackingBusinessObject)
                {
                    ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Deleted;
                }
            }

            return success;
        }

        /// <summary>
        /// Destroy method provides an ability to hard-delete an object from the underlying data store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <returns></returns>
        public static bool Destroy<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            businessObject.ThrowIfNull("businessObject");
            businessObject.CheckReadOnly();

            // Get properties and build a property map
            var propertyMap = Reflector.GetPropertyMap<T>();

            var parameters = GetDeleteParameters(businessObject, propertyMap);

            var response = ObjectFactory.Destroy<T>(parameters);
            var success = response != null && response.RecordsAffected > 0;

            if (success)
            {
                ObjectCache.Remove(businessObject);

                if (businessObject is IChangeTrackingBusinessObject)
                {
                    ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Deleted;
                }
            }

            return success;
        }

        #region IChangeTrackingBusinessObject Methods

        public static bool Save<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            var result = false;
            var setClean = false;
            var isTracked = businessObject is IChangeTrackingBusinessObject;

            if (businessObject.IsNew())
            {
                result = businessObject.Insert();
                setClean = isTracked;
            }
            else if (!isTracked || ((IChangeTrackingBusinessObject)businessObject).IsDirty())
            {
                result = businessObject.Update();
                setClean = isTracked;
            }

            if (result && setClean)
            {
                ((IChangeTrackingBusinessObject)businessObject).ObjectState = ObjectState.Clean;
            }

            return result;
        }

        public static bool IsNew<T>(this T businessObject)
          where T : class, IBusinessObject
        {
            if (businessObject is IChangeTrackingBusinessObject)
            {
                return ((IChangeTrackingBusinessObject)businessObject).ObjectState == ObjectState.New;
            }
            else
            {
                var primaryKey = businessObject.GetPrimaryKey();
                return primaryKey.Values.Sum(v => v == null || v == v.GetType().GetDefault() ? 1 : 0) == primaryKey.Values.Count;
            }
        }

        public static bool IsReadOnly<T>(this T businessObject)
           where T : class, IBusinessObject
        {
            if (businessObject is IChangeTrackingBusinessObject)
            {
                return ((IChangeTrackingBusinessObject)businessObject).ObjectState == ObjectState.ReadOnly;
            }
            return Reflector.GetAttribute<ReadOnlyAttribute>(businessObject.GetType(), false) != null;
        }

        public static bool IsDirty<T>(this T businessObject)
           where T : class, IChangeTrackingBusinessObject
        {
            return businessObject.ObjectState == ObjectState.Dirty;
        }

        public static bool IsDeleted<T>(this T businessObject)
           where T : class, IChangeTrackingBusinessObject
        {
            return businessObject.ObjectState == ObjectState.Deleted;
        }

        #endregion

        #region Parameter Methods

        internal static Param[] GetInsertParameters(object businessObject, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                            .Where(p => p.IsPersistent && (p.IsSimpleType || p.IsSimpleList) && (p.CanWrite || p.IsAutoGenerated))
                            .Select(p => new Param
                            {
                                Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                                Value = GetParameterValue(businessObject, p),
                                DbType = Reflector.ClrToDbType(p.PropertyType),
                                Direction = p.IsAutoGenerated ? ParameterDirection.Output : p.Direction,
                                Source = p.MappedColumnName,
                                IsAutoGenerated = p.IsAutoGenerated,
                                IsPrimaryKey = p.IsPrimaryKey
                            });
            return parameters.ToArray();
        }

        internal static Param[] GetUpdateParameters(object businessObject, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                    .Where(p => p.IsPersistent && (p.IsSimpleType || p.IsSimpleList) && (p.CanWrite || p.IsAutoGenerated))
                    .Select(p => new Param
                    {
                        Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                        Value = GetParameterValue(businessObject, p),
                        DbType = Reflector.ClrToDbType(p.PropertyType),
                        Direction = p.Direction,
                        Source = p.MappedColumnName,
                        IsAutoGenerated = p.IsAutoGenerated,
                        IsPrimaryKey = p.IsPrimaryKey
                    });

            return parameters.ToArray();
        }

        internal static Param[] GetDeleteParameters(object businessObject, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, int statementId = -1)
        {
            var parameters = propertyMap.Values
                    .Where(p => p.CanRead && p.IsPrimaryKey)
                    .Select(p => new Param
                    {
                        Name = (p.ParameterName.NullIfEmpty() ?? p.PropertyName) + (statementId == -1 ? string.Empty : "_" + statementId),
                        Value = GetParameterValue(businessObject, p),
                        Source = p.MappedColumnName,
                        IsPrimaryKey = true
                    });

            return parameters.ToArray();
        }

        private static object GetParameterValue(object businessObject, ReflectedProperty property)
        {
            var result = Reflector.Property.Get(businessObject.GetType(), businessObject, property.PropertyName);
            if (result != null && property.IsSimpleList && property.ElementType != typeof(byte))
            {
                result = ((IEnumerable)result).SafeCast<string>().ToDelimitedString(",");
            }
            return result;
        }

        private static void SetOutputParameterValues<T>(T businessObject, IEnumerable<PropertyInfo> outputProperties, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, IList<Param> parameters)
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
                    Reflector.Property.Set<T>(businessObject, outputProperty.Name, outputPropertyValue);
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
        /// <param name="businessObject"></param>
        /// <param name="includeCacheKey"></param>
        /// <returns></returns>
        public static IDictionary<string, object> GetPrimaryKey<T>(this T businessObject, bool includeCacheKey = false)
            where T : class, IBusinessObject
        {
            // Get properties and build a property map
            var interfaceType = typeof(T);
            if (Reflector.IsMarkerInterface<T>())
            {
                interfaceType = businessObject.GetType();
            }

            string[] primaryKeyProperties = null;
            if (interfaceType != null)
            {
                if (includeCacheKey)
                {
                    primaryKeyProperties = _primaryAndCacheKeys.GetOrAdd(interfaceType, type => ObjectFactory.GetPrimaryKeyProperties(type, true));
                }
                else
                {
                    primaryKeyProperties = _primaryKeys.GetOrAdd(interfaceType, type => ObjectFactory.GetPrimaryKeyProperties(type, false));
                }
            }
            else
            {
                primaryKeyProperties = new string[] { };
            }

            var primaryKey = new SortedDictionary<string, object>();

            for (int i = 0; i < primaryKeyProperties.Length; i++)
            {
                var value = businessObject.Property(primaryKeyProperties[i]);
                primaryKey[primaryKeyProperties[i]] = value;
            }

            return primaryKey;
        }

        public static void GenerateKey<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            var propertyMap = Reflector.GetPropertyMap<T>();
            var generatorKeys = propertyMap.Where(p => p.Value != null && p.Value.Generator != null).Select(p => Tuple.Create(typeof(T), p.Key, p.Value.Generator));
            foreach (var key in generatorKeys)
            {
                var generator = _idGenerators.GetOrAdd(key, k =>
                {
                    var activator = Nemo.Reflection.Activator.CreateDelegate(k.Item3);
                    return (IIdGenerator)activator();
                });

                businessObject.Property(key.Item2.Name, generator.Generate());
            }
        }

        /// <summary>
        /// GetCacheKey method implements cache key generation for a business object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <returns></returns>
        public static string ComputeHash<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            return new CacheKey(businessObject).Value;
        }

        #endregion
        
        #region ReadOnly Methods

        public static T AsReadOnly<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            if (businessObject == null)
            {
                return null;
            }
            return Adapter.Guard(businessObject);
        }

        public static List<T> AsReadOnly<T>(this List<T> businessObjects)
            where T : class, IBusinessObject
        {
            if (businessObjects == null)
            {
                return null;
            }
            return businessObjects.Select(b => b.AsReadOnly()).ToList();
        }

        public static IList<T> AsReadOnly<T>(this IList<T> businessObjects)
            where T : class, IBusinessObject
        {
            if (businessObjects == null)
            {
                return null;
            }
            return businessObjects.Select(b => b.AsReadOnly()).ToArray();
        }

        internal static void CheckReadOnly<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            // Read-only objects can't participate in CRUD
            if (businessObject.IsReadOnly())
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
            where T : class, IBusinessObject
        {
            var data = instance.Serialize(SerializationMode.SerializeAll);
            var value = SerializationExtensions.Deserialize<T>(data);
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
            where T : class, IBusinessObject
        {
            var data = collection.Serialize(SerializationMode.SerializeAll);
            var value = SerializationExtensions.Deserialize<T>(data);
            return value;
        }

        #endregion
    }
}
