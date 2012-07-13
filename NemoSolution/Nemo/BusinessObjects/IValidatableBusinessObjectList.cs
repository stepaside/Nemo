using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    public interface IValidatableBusinessObjectList<T> : IValidatableList
        where T : class
    {
        IEnumerable<T> DataObjectList { get; }
    }
}
