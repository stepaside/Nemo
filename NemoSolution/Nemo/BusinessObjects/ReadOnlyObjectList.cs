using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    public abstract class ReadOnlyObjectList<TSource, TResult> : ReadOnlyCollection<TResult>, IValidatableBusinessObjectList<TSource>
        where TSource : class, IBusinessObject
        where TResult : BusinessObject<TSource>
    {
        public ReadOnlyObjectList(IEnumerable<TResult> items) : base(items.ToList()) { }

        protected new IList<TResult> Items
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        public IEnumerable<TSource> DataObjectList
        {
            get
            {
                return this.GetDataObjects<TResult, TSource>();
            }
        }

        public virtual IEnumerable<Tuple<int, ValidationResult>> Validate()
        {
            return this.DataObjectList.Validate();
        }
    }
}
