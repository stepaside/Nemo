using System.Collections.Generic;
using System.Linq;
using Nemo.Reflection;
using System;
using Nemo.Attributes;

namespace Nemo.UnitOfWork
{
    internal class ChangeNode
    {
        private List<ChangeNode> _nodes = new List<ChangeNode>();
        private ObjectState? _objectState = null;
        private List<string> _listProperties = new List<string>();
        private List<string> _objectProperties = new List<string>();

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
                    Dictionary<ObjectState, int> stateCounts = new Dictionary<ObjectState, int>();
                    foreach (var node in _nodes)
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

        internal List<string> ListProperties
        {
            get
            {
                return _listProperties;
            }
        }

        internal List<string> ObjectProperties
        {
            get
            {
                return _objectProperties;
            }
        }

        internal List<ChangeNode> Nodes
        {
            get
            {
                return _nodes;
            }
        }

        internal ChangeNode Parent
        {
            get;
            set;
        }

        internal ChangeNode ParentObject
        {
            get
            {
                var parent = this.Parent;
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
                return this.Parent == null;
            }
        }

        internal bool IsLeaf
        {
            get
            {
                return this.Parent != null && this.Count == 0;
            }
        }

        internal bool IsEmpty
        {
            get
            {
                return this.Value == null && this.ObjectState == ObjectState.Clean;
            }
        }

        internal bool IsSimpleLeaf
        {
            get
            {
                return this.IsLeaf
                    && (Reflector.IsSimpleType(this.Value.GetType())/* || Reflector.IsXmlDocument(this.Value)*/);
            }
        }

        internal bool IsObject
        {
            get
            {
                return Reflector.IsDataEntity(this.Value);
            }
        }

        internal int Count
        {
            get
            {
                return this.Nodes.Count;
            }
        }
    }
}
