using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public interface IValidatableList
    {
        IEnumerable<Tuple<int, ValidationResult>> Validate();
    }
}
