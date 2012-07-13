using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Nemo.Validation
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class CustomAttribute : ValidationAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        private MethodInfo _validationMethod = null;
        private ICustomValidator _validator = null;
        private string _resourceKey = null;

        public Type ValidationType { get; set; }
        public string ValidationFunction { get; set; }
        public string ClientValidationFunction { get; set; }

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
                    object context = values[0];
                    object propertyValue = values[1];
                    object instance = ((CustomValidatorContext)context).Instance;

                    if (this.ValidationType != null)
                    {
                        // Check if validation type implements IValidator interface.
                        // If it does use an instance of IValidator to validate the property value.
                        if (_validator != null || this.ValidationType.GetInterfaces().Contains(typeof(ICustomValidator)))
                        {
                            if (_validator == null)
                            {
                                _validator = (ICustomValidator)Activator.CreateInstance(this.ValidationType);
                            }
                            _validator.Context = (CustomValidatorContext)context;
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
                            if (_validationMethod == null)
                            {
                                _validationMethod = this.ValidationType.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static).Where(m => m.Name == this.ValidationFunction).FirstOrDefault();
                                if (_validationMethod != null)
                                {
                                    if (_validationMethod.ReturnType != typeof(bool))
                                    {
                                        _validationMethod = null;
                                    }
                                    else
                                    {
                                        var parameters = _validationMethod.GetParameters();
                                        if (parameters.Length != 2 || !instance.GetType().IsAssignableFrom(parameters[0].ParameterType))
                                        {
                                            _validationMethod = null;
                                        }
                                    }
                                }
                            }
                        }

                        if (_validationMethod != null)
                        {
                            return (bool)_validationMethod.Invoke(null, new object[] { instance, propertyValue });
                        }
                    }
                }
            }
            return true;
        }

        public bool IsValid<T>(object instance, T propertyValue)
        {
            return IsValid(new object[] { instance, propertyValue });
        }
    }
}
