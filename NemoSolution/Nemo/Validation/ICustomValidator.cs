using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    /// <summary>
    /// Provides an interface for an external validation object.
    /// </summary>
    public interface ICustomValidator
    {
        ValidationResult Validate(object value);
        CustomValidatorContext Context { get; set; }
    }
}
