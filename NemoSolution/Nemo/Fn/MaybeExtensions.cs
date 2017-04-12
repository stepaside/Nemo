using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Fn
{
    public static class MaybeExtensions
    {
        #region Unit Methods

        /// <summary>
        /// Implements unit function in order to convert any value to the corresponding monad
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Maybe<T> ToMaybe<T>(this T? value) where T : struct
        {
            return value.HasValue ? new Maybe<T>(value.Value) : Maybe<T>.Empty;
        }

        public static Maybe<T> ToMaybe<T>(this T value)
        {
            if (value is ValueType) return new Maybe<T>(value);
            return ReferenceEquals(value, null) ? Maybe<T>.Empty : new Maybe<T>(value);
        }

        #endregion

        #region Bind Methods

        /// <summary>
        /// Implements bind function in order to provide the ability to achieve function composition 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="maybe"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static Maybe<U> Select<T, U>(this Maybe<T> maybe, Func<T, Maybe<U>> func)
        {
            return !maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value);
        }

        public static Maybe<U> Select<T, U>(this Maybe<T> maybe, Func<T, U> func)
        {
            return !maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value).ToMaybe();
        }

        #endregion

        #region SelectMany Methods

        /// <summary>
        /// SelectMany implementation allows the use of LINQ syntax with the given monad
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="maybe"></param>
        /// <param name="func"></param>
        /// <param name="select"></param>
        /// <returns></returns>
        public static Maybe<V> SelectMany<T, U, V>(this Maybe<T> maybe, Func<T, Maybe<U>> func, Func<T, U, V> select)
        {
            return maybe.Select(a => func(a).Select(b => select(a, b).ToMaybe()));
        }

        #endregion

        #region Utility Methods

        public static Maybe<T> Do<T>(this Maybe<T> maybe, Action<T> action)
        {
            if (maybe.HasValue)
            {
                action(maybe.Value);
            }
            return maybe;
        }

        public static Maybe<T> DoIfEmpty<T>(this Maybe<T> maybe, Action<T> action)
        {
            if (!maybe.HasValue)
            {
                action(maybe.Value);
            }
            return maybe;
        }

        public static Maybe<U> SelectIfEmpty<T, U>(this Maybe<T> maybe, Func<T, Maybe<U>> func)
        {
            return maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value);
        }

        public static Maybe<U> SelectIfEmpty<T, U>(this Maybe<T> maybe, Func<T, U> func)
        {
            return maybe.HasValue ? Maybe<U>.Empty : func(maybe.Value).ToMaybe();
        }

        #endregion
    }
}
