using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Collections
{
    public interface ISharedEnumerable<T> : IEnumerable<T>
    {
        NextResult<T> Next();
    }

    public class NextResult<T>
    {
        internal NextResult(bool success, T value)
        {
            Success = success;
            Value = value;
        }

        public bool Success { get; private set; }
        public T Value { get; private set; }
    }
}
