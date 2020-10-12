using Nemo.Extensions;
using Nemo.Reflection;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo
{
    public class DefaultAggregatePropertyMapper<TAggregateRoot, T> : IAggregatePropertyMapper<TAggregateRoot, T>
        where TAggregateRoot : class
        where T : class
    {
        private TAggregateRoot _current;

        public TAggregateRoot Map(TAggregateRoot aggregate, T entity)
        {
            // Terminating call.  Since we can return null from this function
            // we need to be ready for Nemo to callback later with null
            // parameters
            if (aggregate == null)
            {
                return _current;
            }

            var map = Reflector.GetPropertyMap<TAggregateRoot>();
            var collectionProperty = map.Values.FirstOrDefault(v => v.IsObjectList && typeof(T).InheritsFrom(v.ElementType));
            if (collectionProperty != null)
            {
                // Is this the same aggregate root as the current one we're processing
                if (_current != null && _current.GetPrimaryKey().SequenceEqual(aggregate.GetPrimaryKey()))
                {
                    // Yes, just add this entity to the current aggregate's collection of type T
                    if (_current.PropertyTryGet(collectionProperty.PropertyName, out var collection))
                    {
                        ((IList)collection).Add(entity);
                    }

                    // Return null to indicate we're not done with this aggregate yet
                    return null;
                }

                // This is a different aggregate to the current one, or this is the 
                // first time through and we don't have an aggregate yet

                // Save the current aggregate root
                var prev = _current;

                // Setup the new current aggregate root
                _current = aggregate;
                if (collectionProperty.CanWrite)
                {
                    _current.Property(collectionProperty.PropertyName, new List<T>());
                }
                ((IList)_current.Property(collectionProperty.PropertyName)).Add(entity);

                // Return the now populated previous aggregate root (or null if first time through)
                return prev;
            }

            var singleProperty = map.Values.FirstOrDefault(v => v.PropertyType == typeof(T) && v.CanWrite);
            if (singleProperty != null)
            {
                // Save the current aggregate root
                var prev = _current;

                // Setup the new current aggregate root
                _current = aggregate;

                _current.Property(collectionProperty.PropertyName, entity);

                // Return the now populated previous aggregate root (or null if first time through)
                return prev;
            }

            return _current;
        }
    }
}
