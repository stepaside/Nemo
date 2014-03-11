using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Utilities;

namespace Nemo.Reflection
{
    internal class ReflectedType
    {
        internal ReflectedType(Type type)
        {
            TypeName = type.Name;
            FullTypeName = type.FullName;
            var interfaceType = Reflector.ExtractInterface(type);
            if (interfaceType != null)
            {
                InterfaceTypeName = interfaceType.FullName;
            }
            IsArray = type.IsArray;
            IsSimpleList = Reflector.IsSimpleList(type);
            IsDataEntity = Reflector.IsDataEntity(type);
            Type elementType;
            IsDataEntityList = Reflector.IsDataEntityList(type, out elementType);
            ElementType = elementType;
            if (IsDataEntityList)
            {
                IsListInterface = type.GetGenericTypeDefinition() == typeof(IList<>);
            }
            IsSimpleType = Reflector.IsSimpleType(type);
            IsTypeUnion = Reflector.IsTypeUnion(type);
            if (IsTypeUnion)
            {
                GenericArguments = type.GetGenericArguments();
            }
            IsTuple = Reflector.IsTuple(type);
            IsList = Reflector.IsList(type);
            IsDictionary = Reflector.IsDictionary(type);
            IsNullableType = Reflector.IsNullableType(type);
            IsMarkerInterface = Reflector.IsMarkerInterface(type);
            HashCode = type.GetHashCode();
            if (IsDataEntity)
            {
                IsCacheableEntity = Reflector.IsCacheableDataEntity(type);
            }
            IsGenericType = type.IsGenericType;
            IsInterface = type.IsInterface;
            IsAnonymous = Reflector.IsAnonymousType(type);
        }

        public string TypeName
        {
            get;
            private set;
        }

        public bool IsSimpleList
        {
            get;
            private set;
        }

        public bool IsDataEntity
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

        public Type ElementType
        {
            get;
            private set;
        }

        public bool IsSimpleType
        {
            get;
            private set;
        }

        public bool IsTypeUnion
        {
            get;
            private set;
        }

        public bool IsTuple
        {
            get;
            private set;
        }

        public bool IsNullableType
        {
            get;
            private set;
        }

        public string FullTypeName
        {
            get;
            private set;
        }

        public string InterfaceTypeName
        {
            get;
            private set;
        }

        public bool IsList
        {
            get;
            private set;
        }

        public bool IsDictionary
        {
            get;
            private set;
        }

        public string XmlElementName
        {
            get;
            internal set;
        }

        public bool IsMarkerInterface
        {
            get;
            private set;
        }

        public int HashCode
        {
            get;
            private set;
        }

        public bool IsCacheableEntity
        {
            get;
            private set;
        }

        public Type[] GenericArguments
        {
            get;
            private set;
        }

        public bool IsGenericType
        {
            get;
            private set;
        }

        public bool IsInterface
        {
            get;
            private set;
        }

        public bool IsAnonymous
        {
            get;
            private set;
        }

        public bool IsArray
        {
            get;
            private set;
        }
    }
}
