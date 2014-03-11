using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Attributes;
using System.ComponentModel;

namespace Nemo
{
    public interface IDataEntity
    {
    }

    public enum ObjectState { Clean, New, Dirty, Deleted, ReadOnly, DirtyPrimaryKey }

    public interface ITrackableDataEntity : IDataEntity
    {
        [DoNotPersist, DoNotSelect]
        ObjectState ObjectState
        {
            get;
            set;
        }
    }
}
