using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    public abstract class BusinessObject<T> : IValidatableBusinessObject<T> where T : class, IBusinessObject
    {
        public abstract T DataObject { get; }

        public virtual ValidationResult Validate()
        {
            if (this.DataObject != null)
            {
                return this.DataObject.Validate();
            }
            return new ValidationResult();
        }
    }
}
