using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo
{
    public interface IExecutionContext
    {
        bool Exists(string name);
        object Get(string name);
        bool TryGet(string name, out object value);
        void Set(string name, object value);
        void Remove(string name);
    }
}
