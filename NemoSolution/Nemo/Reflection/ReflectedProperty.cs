﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Nemo.Attributes;
using Nemo.Fn;

namespace Nemo.Reflection
{
    public class ReflectedProperty
    {
        public ReflectedProperty(PropertyInfo property, int position = -1, bool readAttributes = true)
        {
            PropertyName = property.Name;
            PropertyType = property.PropertyType;
            IsPersistent = Maybe<bool>.Empty;
            IsSelectable = Maybe<bool>.Empty;
            IsSerializable = Maybe<bool>.Empty;
            IsBinary = property.PropertyType == typeof(byte[]);
            IsSimpleList = !IsBinary && Reflector.IsSimpleList(property.PropertyType);
            IsDataEntity = Reflector.IsDataEntity(property.PropertyType);
            
            Type elementType;
            IsDataEntityList = Reflector.IsDataEntityList(property.PropertyType, out elementType);
            ElementType = elementType;
                        
            if (IsDataEntityList)
            {
                IsList = true;
                IsListInterface = property.PropertyType.GetGenericTypeDefinition() == typeof(IList<>);
            }
            else
            {
                IsList = !IsBinary && Reflector.IsList(property.PropertyType);
                if (IsList)
                {
                    ElementType = Reflector.ExtractCollectionElementType(property.PropertyType);
                    IsListInterface = property.PropertyType.GetGenericTypeDefinition() == typeof(IList<>);
                }
            }

            IsSimpleType = !IsBinary && Reflector.IsSimpleType(property.PropertyType);
            IsNullableType = Reflector.IsNullableType(property.PropertyType);
            CanWrite = property.CanWrite;
            CanRead = property.CanRead;
            Position = position;
            
            if (readAttributes)
            {
                if (IsListInterface)
                {
                    Sorted = property.GetCustomAttributes(typeof(SortedAttribute), false).Cast<SortedAttribute>().FirstOrDefault();
                    Distinct = property.GetCustomAttributes(typeof(DistinctAttribute), false).Cast<DistinctAttribute>().FirstOrDefault();
                }
                
                MappedColumnName = MapColumnAttribute.GetMappedColumnName(property);
                MappedPropertyName = MapPropertyAttribute.GetMappedPropertyName(property);
                
                var items = property.GetCustomAttributes(true).OfType<PropertyAttribute>();
                foreach (var item in items)
                {
                    var primaryKeyAttribute = item as PrimaryKeyAttribute;
                    if (primaryKeyAttribute != null)
                    {
                        IsPrimaryKey = true;
                        KeyPosition = primaryKeyAttribute.Position;
                    }
                    else
                    {
                        var generategAttribute = item as Generate.UsingAttribute;
                        if (generategAttribute != null)
                        {
                            Generator = generategAttribute.Type;
                        }
                        else if (item is Generate.NativeAttribute)
                        {
                            IsAutoGenerated = true;
                        }
                        else
                        {
                            var referencesAttribute = item as ReferencesAttribute;
                            if (referencesAttribute != null)
                            {
                                Parent = referencesAttribute.Parent;
                                RefPosition = referencesAttribute.Position;
                            }
                            else
                            {
                                var parameterAttribute = item as ParameterAttribute;
                                if (parameterAttribute != null)
                                {
                                    ParameterName = parameterAttribute.Name;
                                    Direction = parameterAttribute.Direction;
                                }
                                else if (item is DoNotPersistAttribute)
                                {
                                    IsPersistent = false;
                                }
                                else if (item is DoNotSelectAttribute)
                                {
                                    IsSelectable = false;
                                }
                                else if (item is DoNotSerializeAttribute)
                                {
                                    IsSerializable = false;
                                }
                            }
                        }
                    }
                }
            }

            if (!IsPersistent.HasValue)
            {
                IsPersistent = true;
            }

            if (!IsSelectable.HasValue)
            {
                IsSelectable = true;
            }

            if (!IsSerializable.HasValue)
            {
                IsSerializable = true;
            }
        }

        public bool IsSimpleList
        {
            get;
            private set;
        }

        public bool IsDataEntityList
        {
            get;
            private set;
        }

        public bool IsListInterface
        {
            get;
            private set;
        }

        public bool IsDataEntity
        {
            get;
            private set;
        }

        public bool IsSimpleType
        {
            get;
            private set;
        }
        
        public Maybe<bool> IsPersistent
        {
            get;
            internal set;
        }

        public Maybe<bool> IsSerializable
        {
            get;
            internal set;
        }

        public bool IsPrimaryKey
        {
            get;
            internal set;
        }

        public bool IsAutoGenerated
        {
            get;
            internal set;
        }

        public Type Generator
        {
            get;
            internal set;
        }

        public Type Parent
        {
            get;
            internal set;
        }

        public Maybe<bool> IsSelectable
        {
            get;
            internal set;
        }
      
        public string ParameterName
        {
            get;
            internal set;
        }

        public ParameterDirection Direction
        {
            get;
            internal set;
        }

        public string PropertyName
        {
            get;
            private set;
        }

        public Type PropertyType
        {
            get;
            private set;
        }

        public string MappedColumnName
        {
            get;
            internal set;
        }

        public string MappedPropertyName
        {
            get;
            internal set;
        }

        public bool CanWrite
        {
            get;
            private set;
        }

        public bool CanRead
        {
            get;
            private set;
        }

        public Type ElementType
        {
            get;
            private set;
        }

        public bool IsNullableType
        {
            get;
            private set;
        }

        public bool IsList
        {
            get;
            private set;
        }

        public SortedAttribute Sorted
        {
            get;
            internal set;
        }

        public DistinctAttribute Distinct
        {
            get;
            internal set;
        }

        public int Position
        {
            get;
            private set;
        }

        internal int KeyPosition
        {
            get;
            set;
        }

        internal int RefPosition
        {
            get;
            set;
        }

        internal bool IsBinary
        {
            get;
            set;
        }
    }
}
