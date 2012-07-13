using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Fn
{
    public static class MaybeExtensions
    {
        public static Maybe<T> ToMaybe<T>(this T? value) where T : struct
        {
            if (value.HasValue)
            {
                return new Maybe<T>(value.Value);
            }
            return Maybe<T>.Empty;
        }

        public static Maybe<T> ToMaybe<T>(this T value)
        {
            if (!(value is ValueType))
            {
                if (object.ReferenceEquals(value, null))
                {
                    return Maybe<T>.Empty;
                }
            }
            return new Maybe<T>(value);
        }

        public static Maybe<U> SelectMany<T, U>(this Maybe<T> maybe, Func<T, Maybe<U>> func)
        {
            return !maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value);
        }

        public static Maybe<V> SelectMany<T, U, V>(this Maybe<T> maybe, Func<T, Maybe<U>> func, Func<T, U, V> select)
        {
            if (!maybe.HasValue)
            {
                return Maybe<V>.Empty;
            }

            Maybe<U> u = func(maybe.Value);
            return !u.HasValue ? Maybe<V>.Empty : select(maybe.Value, u.Value).ToMaybe();
        }

        public static Maybe<U> Select<U, T>(this Maybe<T> maybe, Func<T, U> select)
        {
            return !maybe.HasValue ? Maybe<U>.Empty : select(maybe.Value).ToMaybe();
        }

        public static Maybe<T> Do<T>(this Maybe<T> maybe, Action<T> action)
        {
            if (maybe.HasValue)
            {
                action(maybe.Value);
            }
            return maybe;
        }

        public static Maybe<T> OnEmpty<T>(this Maybe<T> maybe, Action action)
        {
            if (!maybe.HasValue)
            {
                action();
            }
            return maybe;
        }

        public static Maybe<U> OnEmpty<T, U>(this Maybe<T> maybe, Func<T, U> func)
        {
            return maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value).ToMaybe();
        }
    }
}
