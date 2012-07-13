using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Nemo.Extensions;

namespace Nemo.Validation
{
    public class ValidationResult : Dictionary<string, List<ValidationError>>
    {
        public ValidationResult() : base() { }
        public ValidationResult(IDictionary<string, List<ValidationError>> dictionary) : base(dictionary) { }

        public override string ToString()
        {
            return this.SelectMany(k => k.Value).OrderBy(e => e.PropertyName).Select(e => string.Format("{0}: {1}", e.PropertyName, e.ErrorMessage)).ToDelimitedString("\n\r");
        }
    }
}
