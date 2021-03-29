using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections
{
    internal class TypeArray : IEqualityComparer<TypeArray>
    {
        private readonly IList<Type> _types;
        private readonly Lazy<int> _hashCode;

        public TypeArray(IList<Type> types)
        {
            _types = types;
            _hashCode = new Lazy<int>(() => _types.Select(t => t.FullName).ToDelimitedString("|").GetHashCode(), true);
        }

        public IList<Type> Types
        {
            get
            {
                return _types;
            }
        }

        public bool Equals(TypeArray x, TypeArray y)
        {
            return (x._types?.SequenceEqual(y._types, EqualityComparer<Type>.Default)).GetValueOrDefault();
        }

        public int GetHashCode(TypeArray obj)
        {
            return obj._hashCode.Value;
        }
    }
}
