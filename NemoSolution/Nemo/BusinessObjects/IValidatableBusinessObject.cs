using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    /// <summary>
    /// Provides internal validation for BOL objects.
    /// </summary>
    public interface IValidatableBusinessObject<T> : IValidatable
        where T : class
    {
        T DataObject { get; }
    }
}
