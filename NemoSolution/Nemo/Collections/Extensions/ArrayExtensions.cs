using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Collections.Extensions
{
    public static class ArrayExtensions
    {
        public static T[] Slice<T>(this T[] source, int start, int end)
        {
            // Handles negative ends.
            if (end < 0)
            {
                end = source.Length + end;
            }
            int len = end - start;

            // Return new array.
            T[] res = new T[len];
            Buffer.BlockCopy(source, start, res, 0, len);
            return res;
        }

        public static T[] ToArray<T>(this ArraySegment<T> array)
        {
            var buffer = new T[array.Count - array.Offset];
            Buffer.BlockCopy(array.Array, array.Offset, buffer, 0, array.Count - array.Offset);
            return buffer;
        }

        public static ArraySegment<T> GetSegment<T>(this T[] array, int from, int count)
        {
            return new ArraySegment<T>(array, from, count);
        }

        public static ArraySegment<T> GetSegment<T>(this T[] array, int from)
        {
            return GetSegment(array, from, array.Length - from);
        }

        public static ArraySegment<T> GetSegment<T>(this T[] array)
        {
            return new ArraySegment<T>(array);
        }
    }
}
