using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    /// <summary>
    /// Provides an abstract base class for an external validation object.
    /// </summary>
    public abstract class CustomValidator
    {
        public CustomValidator(CustomValidatorContext context)
        {
            this.Context = context;
        }

        public abstract ValidationResult Validate(object value);
        public CustomValidatorContext Context { get; private set; }
    }
}
