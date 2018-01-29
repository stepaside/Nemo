using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Fn.Extensions
{
    public static class FunctionExtensions
    {
        #region Function Currying Methods

        public static Func<T, TResult> Curry<T, TResult>(this Func<T, TResult> func)
        {
            return value => func(value);
        }

        public static Func<TResult> Curry<T, TResult>(this Func<T, TResult> func, T value)
        {
            return () => func(value);
        }

        public static Func<T2, TResult> Curry<T1, T2, TResult>(this Func<T1, T2, TResult> func, T1 value1)
        {
            return value2 => func(value1, value2);
        }

        public static Func<TResult> Curry<T1, T2, TResult>(this Func<T1, T2, TResult> func, T1 value1, T2 value2)
        {
            return () => func(value1, value2);
        }

        public static Func<T1, Func<T2, TResult>> Curry<T1, T2, TResult>(this Func<T1, T2, TResult> func)
        {
            return value1 => value2 => func(value1, value2);
        }

        public static Func<T2, T3, TResult> Curry<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> func, T1 value1)
        {
            return (value2, value3) => func(value1, value2, value3);
        }

        public static Func<T3, TResult> Curry<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> func, T1 value1, T2 value2)
        {
            return (value3) => func(value1, value2, value3);
        }

        public static Func<TResult> Curry<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> func, T1 value1, T2 value2, T3 value3)
        {
            return () => func(value1, value2, value3);
        }

        public static Func<T1, Func<T2, Func<T3, TResult>>> Curry<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> func)
        {
            return value1 => value2 => value3 => func(value1, value2, value3);
        }

        public static Func<T2, T3, T4, TResult> Curry<T1, T2, T3, T4, TResult>(this Func<T1, T2, T3, T4, TResult> func, T1 value1)
        {
            return (value2, value3, value4) => func(value1, value2, value3, value4);
        }

        public static Func<T3, T4, TResult> Curry<T1, T2, T3, T4, TResult>(this Func<T1, T2, T3, T4, TResult> func, T1 value1, T2 value2)
        {
            return (value3, value4) => func(value1, value2, value3, value4);
        }

        public static Func<T4, TResult> Curry<T1, T2, T3, T4, TResult>(this Func<T1, T2, T3, T4, TResult> func, T1 value1, T2 value2, T3 value3)
        {
            return (value4) => func(value1, value2, value3, value4);
        }

        public static Func<TResult> Curry<T1, T2, T3, T4, TResult>(this Func<T1, T2, T3, T4, TResult> func, T1 value1, T2 value2, T3 value3, T4 value4)
        {
            return () => func(value1, value2, value3, value4);
        }

        public static Func<T1, Func<T2, Func<T3, Func<T4, TResult>>>> Curry<T1, T2, T3, T4, TResult>(this Func<T1, T2, T3, T4, TResult> func)
        {
            return value1 => value2 => value3 => value4 => func(value1, value2, value3, value4);
        }

        public static Func<T2, T3, T4, T5, TResult> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func, T1 value1)
        {
            return (value2, value3, value4, value5) => func(value1, value2, value3, value4, value5);
        }

        public static Func<T3, T4, T5, TResult> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func, T1 value1, T2 value2)
        {
            return (value3, value4, value5) => func(value1, value2, value3, value4, value5);
        }

        public static Func<T4, T5, TResult> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func, T1 value1, T2 value2, T3 value3)
        {
            return (value4, value5) => func(value1, value2, value3, value4, value5);
        }

        public static Func<T5, TResult> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func, T1 value1, T2 value2, T3 value3, T4 value4)
        {
            return (value5) => func(value1, value2, value3, value4, value5);
        }

        public static Func<TResult> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5)
        {
            return () => func(value1, value2, value3, value4, value5);
        }

        public static Func<T1, Func<T2, Func<T3, Func<T4, Func<T5, TResult>>>>> Curry<T1, T2, T3, T4, T5, TResult>(this Func<T1, T2, T3, T4, T5, TResult> func)
        {
            return value1 => value2 => value3 => value4 => value5 => func(value1, value2, value3, value4, value5);
        }

        #endregion
    }
}
