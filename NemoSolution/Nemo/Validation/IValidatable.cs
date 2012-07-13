using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public interface IValidatable
    {
        ValidationResult Validate();
    }
}
