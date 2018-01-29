using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo
{
    public class OperationResponse : IDisposable
    {
        public object Value { get; internal set; }
        public Exception Exception { get; internal set; }
        public bool HasErrors
        {
            get
            {
                return Exception != null;
            }
        } 
        public OperationReturnType ReturnType { get; internal set; }
        public int RecordsAffected { get; internal set; }

        public void Dispose()
        {
            if (Value != null && Value is IDisposable)
            {
                ((IDisposable)Value).Dispose();
            }
        }
    }
}
