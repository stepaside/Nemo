using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo
{
    public enum OperationReturnType : byte
    {
        Guess, 
        NonQuery, 
        MultiResult, 
        SingleResult, 
        SingleRow, 
        Scalar, 
        DataSet, 
        DataTable
    }
}
