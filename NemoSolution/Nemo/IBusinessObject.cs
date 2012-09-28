using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Attributes;
using System.ComponentModel;

namespace Nemo
{
    public interface IBusinessObject
    {
    }

    public enum ObjectState { Clean, New, Dirty, Deleted, ReadOnly, DirtyPrimaryKey }

    public interface IChangeTrackingBusinessObject : IBusinessObject
    {
        [DoNotPersist, DoNotSelect]
        ObjectState ObjectState
        {
            get;
            set;
        }
    }
}
