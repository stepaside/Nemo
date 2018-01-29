using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public class ValidationError
    {
        public ValidationError() { }
        
        public ValidationError(string errorMessage) 
        {
            this.ErrorMessage = errorMessage;
        }

        public ValidationError(string errorMessage, object targetInstance, string propertyName)
            : this(errorMessage)
        {
            this.TargetInstance = targetInstance;
            this.PropertyName = propertyName;
        }

        public ValidationError(string errorMessage, object targetInstance, string propertyName, string validationType, SeverityType severityType)
            : this(errorMessage, targetInstance, propertyName)
        {
            this.ValidationType = validationType;
            this.SeverityType = severityType;
        }

        public string ErrorMessage { get; internal set; }
        public object TargetInstance { get; internal set; }
        public string PropertyName { get; internal set; }
        public string ValidationType { get; internal set; }
        public SeverityType SeverityType { get; internal set; }
    }
}
