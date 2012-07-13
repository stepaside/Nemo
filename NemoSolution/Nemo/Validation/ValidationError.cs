using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public enum SeverityType
    {
        Error,
        Warning,
        Infomration,
        Debug
    }

    public interface ISeverityTypeProvider
    {
        SeverityType SeverityType
        {
            get;
            set;
        }
    }

    public interface IResourceKeyProvider
    {
        string ResourceKey
        {
            get;
            set;
        }
    }

    public class ValidationError
    {
        public ValidationError() { }
        
        public ValidationError(string errorMessage) 
        {
            this.ErrorMessage = errorMessage;
        }

        public ValidationError(string errorMessage, object targetObject, string propertyName)
            : this(errorMessage)
        {
            this.Object = targetObject;
            this.PropertyName = propertyName;
        }

        public ValidationError(string errorMessage, object targetObject, string propertyName, string validationType, SeverityType severityType)
            : this(errorMessage, targetObject, propertyName)
        {
            this.ValidationType = validationType;
            this.SeverityType = severityType;
        }

        public string ErrorMessage { get; internal set; }
        public object Object { get; internal set; }
        public string PropertyName { get; internal set; }
        public string ValidationType { get; internal set; }
        public SeverityType SeverityType { get; internal set; }
    }
}
