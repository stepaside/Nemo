using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Nemo.Collections.Extensions;
using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    public abstract class ObjectList<TSource, TResult> : Collection<TResult>, IValidatableBusinessObjectList<TSource>
        where TSource : class, IBusinessObject
        where TResult : BusinessObject<TSource>
    {
        private Func<IList<TSource>> _loader;
        private IList<TSource> _items;
        private Func<TSource, TResult> _mapper;

        public ObjectList(Func<IList<TSource>> loader, Func<TSource, TResult> mapper) : base() 
        {
            _loader = loader;
            _mapper = mapper;
            _items = new List<TSource>();
        }

        public ObjectList(IList<TSource> items, Func<TSource, TResult> mapper) : base(items.Select(i => mapper(i)).ToList())
        {
            _items = items;
            _mapper = mapper;
        }

        protected new IList<TResult> Items
        {
            get
            {
                throw new NotSupportedException();
            }
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

            _items = _loader();
            int index = 0;
            _items.Run(i => InsertDuringLoad(index++, _mapper(i)));
            return true;
        }

        private void InsertDuringLoad(int index, TResult item)
        {
            base.InsertItem(index, item);
        }

        public bool IsValid()
        {
            return _items != null && _items.Count == this.Count;
        }

        protected override void InsertItem(int index, TResult item)
        {
            base.InsertItem(index, item);
            _items.Insert(index, item.DataObject);
        }

        protected override void ClearItems()
        {
            base.ClearItems();
            _items.Clear();
        }

        protected override void RemoveItem(int index)
        {
            base.RemoveItem(index);
            _items.RemoveAt(index);
        }

        protected override void SetItem(int index, TResult item)
        {
            base.SetItem(index, item);
            _items[index] = item.DataObject;
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
