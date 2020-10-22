using System.Collections.Generic;
using System.Linq;
using Nemo.Reflection;
using System;
using Nemo.Attributes;

namespace Nemo.UnitOfWork
{
    internal class ChangeNode
    {
        private ObjectState? _objectState;

        internal Type Type
        {
            get;
            set;
        }

        internal string PropertyName
        {
            get;
            set;
        }

        internal ReflectedProperty Property
        {
            get;
            set;
        }

        internal object Value 
        { 
            get; 
            set; 
        }

        internal int? Index
        {
            get;
            set;
        }
        
        internal ObjectState ObjectState
        {
            get
            {
                if (!_objectState.HasValue)
                {
                    var stateCounts = new Dictionary<ObjectState, int>();
                    foreach (var node in Nodes)
                    {
                        if (!stateCounts.ContainsKey(node.ObjectState))
                        {
                            stateCounts.Add(node.ObjectState, 1);
                        }
                        else
                        {
                            stateCounts[node.ObjectState] += 1;
                        }
                    }
                    if (stateCounts.Count > 1 || (this.IsRoot && stateCounts.Count == 1))
                    {
                        _objectState = ObjectState.Dirty;
                    }
                    else if (stateCounts.Count == 0)
                    {
                        _objectState = ObjectState.Clean;
                    }
                    else
                    {
                        _objectState = stateCounts.First().Key;
                    }
                }
                return _objectState.Value;
            }
            set
            {
                _objectState = value;
            }
        }

        internal List<string> ListProperties { get; } = new List<string>();

        internal List<string> ObjectProperties { get; } = new List<string>();

        internal List<ChangeNode> Nodes { get; } = new List<ChangeNode>();

        internal ChangeNode Parent
        {
            get;
            set;
        }

        internal ChangeNode ParentObject
        {
            get
            {
                var parent = Parent;
                while (parent != null)
                {
                    if (parent.IsObject)
                    {
                        return parent;
                    }
                    parent = parent.Parent;
                }
                return null;
            }
        }

        internal bool IsRoot
        {
            get
            {
                return Parent == null;
            }
        }

        internal bool IsLeaf
        {
            get
            {
                return Parent != null && Count == 0;
            }
        }

        internal bool IsEmpty
        {
            get
            {
                return Value == null && ObjectState == ObjectState.Clean;
            }
        }

        internal bool IsSimpleLeaf
        {
            get
            {
                return IsLeaf && Value != null && Reflector.IsSimpleType(Value.GetType());
            }
        }

        internal bool IsObject
        {
            get
            {
                return Reflector.IsDataEntity(Value) || (Value != null && !Reflector.IsSimpleType(Value.GetType()));
            }
        }

        internal int Count
        {
            get
            {
                return Nodes.Count;
            }
        }
    }
}
