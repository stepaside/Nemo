using System;
using System.Collections.Generic;

namespace Nemo.Collections
{
    internal sealed class TypeArray : IEquatable<TypeArray>
    {
        private readonly IList<Type> _types;
        private readonly int _hashCode;

        public TypeArray(IList<Type> types)
        {
            _types = types;

            unchecked
            {
                var hashCode = 17;
                if (types != null)
                {
                    for (var i = 0; i < types.Count; i++)
                    {
                        hashCode = (hashCode * 31) + (types[i]?.GetHashCode() ?? 0);
                    }
                }
                _hashCode = hashCode;
            }
        }

        public IList<Type> Types
        {
            get
            {
                return _types;
            }
        }

        public bool Equals(TypeArray other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other == null || _hashCode != other._hashCode) return false;
            if (_types == null || other._types == null) return _types == other._types;
            if (_types.Count != other._types.Count) return false;

            for (var i = 0; i < _types.Count; i++)
            {
                if (_types[i] != other._types[i]) return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TypeArray);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }
    }
}
