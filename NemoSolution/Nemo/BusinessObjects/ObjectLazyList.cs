using System;
using System.Collections.Generic;
using System.Linq;
using Nemo.Extensions;
using Nemo.Validation;
using Nemo.Collections;

namespace Nemo.BusinessObjects
{
    public abstract class ObjectLazyList<TSource, TResult> : MapList<TSource, TResult>, IValidatableBusinessObjectList<TSource>
        where TSource : class, IBusinessObject
        where TResult : BusinessObject<TSource>
    {
        private Func<IList<TSource>> _loader;
        private bool _isValid;

        public ObjectLazyList(Func<IList<TSource>> loader, Func<TSource, TResult> mapper)
            : this(new List<TSource>(), mapper)
        {
            _loader = loader;
        }

        public ObjectLazyList(IList<TSource> items, Func<TSource, TResult> mapper)
            : base(items, mapper)
        {
            _isValid = items != null;
        }

        public bool Load()
        {
            if (_loader == null)
            {
                return false;
            }

            if (this.Count > 0)
            {
                this.Clear();
            }

            var items = _loader();
            InitializeItems(items);
            _isValid = items != null;
            return true;
        }

        public bool IsValid()
        {
            return _isValid;
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
