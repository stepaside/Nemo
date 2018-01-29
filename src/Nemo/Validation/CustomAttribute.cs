using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;
using Nemo.Reflection;

namespace Nemo.Validation
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class CustomAttribute : ValidationAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        private RuntimeMethodHandle? _validationMethodHandle = null;
        private CustomValidator _validator = null;
        private string _resourceKey = null;

        public Type ValidatorType { get; set; }
        public string ValidationFunction { get; set; }

        public SeverityType SeverityType
        {
            get;
            set;
        }

        public string ResourceKey
        {
            get
            {
                return _resourceKey;
            }
            set
            {
                _resourceKey = value;
                this.SetErrorMessage();
            }
        }

        public override bool IsValid(object value)
        {
            if (value != null && value.GetType().IsArray)
            {
                object[] values = (object[])value;
                if (values.Length > 1)
                {
                    var context = (CustomValidatorContext)values[0];
                    object propertyValue = values[1];

                    if (this.ValidatorType != null)
                    {
                        // Check if validation type implements IValidator interface.
                        // If it does use an instance of IValidator to validate the property value.
                        if (_validator != null || typeof(CustomValidator).IsAssignableFrom(ValidatorType))
                        {
                            if (_validator == null)
                            {
                                _validator = (CustomValidator)Nemo.Reflection.Activator.New(this.ValidatorType, context);
                            }
                            var result = _validator.Validate(propertyValue);
                            return result == null || result.Count == 0;
                        }
                        
                        // Otherwise use a static function if one is provided
                        if (!string.IsNullOrEmpty(this.ValidationFunction))
                        {
                            // If validation method is null 
                            // find a static public method in the validation type provided
                            // which returns boolean and accepts two arguments: 
                            // an object instance and a value to validate
                            if (_validationMethodHandle == null)
                            {
                                var validationMethod = this.ValidatorType.GetMethod(this.ValidationFunction);
                                if (validationMethod != null && validationMethod.ReturnType == typeof(bool))
                                {
                                    var parameters = validationMethod.GetParameters();
                                    if (parameters.Length == 2 && parameters[0].ParameterType == typeof(CustomValidatorContext))
                                    {
                                        _validationMethodHandle = validationMethod.MethodHandle;
                                    }
                                }
                            }
                        }

                        if (_validationMethodHandle.HasValue)
                        {
                            var validator = Reflector.Method.CreateDelegate(_validationMethodHandle.Value);
                            return (bool)validator(null, new object[] { context, propertyValue });
                        }
                    }
                }
            }
            return true;
        }

        public bool IsValid<T>(object instance, string propertyName, T propertyValue)
        {
            return IsValid(new object[] { new CustomValidatorContext(instance, propertyName, this), propertyValue });
        }
    }
}
