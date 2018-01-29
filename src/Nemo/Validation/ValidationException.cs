using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public class ValidationException : ApplicationException
    {
        public ValidationException()
            : base()
        {
        }

        public ValidationException(string message)
            : base(message)
        {
        }

        public ValidationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public ValidationException(ValidationResult validationResult)
            : this(validationResult != null ? validationResult.ToString() : string.Empty)
        {
            this.ValidationResult = validationResult;
        }

        public ValidationException(ValidationResult validationResult, Exception innerException)
            : this(validationResult != null ? validationResult.ToString() : string.Empty, innerException)
        {
            this.ValidationResult = validationResult;
        }

        public ValidationResult ValidationResult
        {
            get;
            private set;
        }
    }
}
