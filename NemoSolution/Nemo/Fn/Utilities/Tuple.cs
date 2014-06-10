using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections.Extensions;
using Nemo.Extensions;

namespace Nemo.Fn.Utilities
{
    public interface ITuple
    {
        int Size { get; }
        bool Contains<T>(T value);
        bool Contains<T>(T value, IEqualityComparer<T> comparer);
        T Get<T>(int index);
        string GetString();
    }

    [Serializable]
    public class Tuple<T1> : ITuple
    {
        public Tuple(T1 item1)
        {
            Item1 = item1;
        }

        public readonly T1 Item1;

        int ITuple.Size
        {
            get
            {
                return 1;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0}", Item1);
        }

        public virtual bool Contains<T>(T value)
        {
            return typeof(T) == typeof(T1) && Equals(Item1, value);
        }

        public virtual bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return typeof(T) == typeof(T1) && comparer.Equals(Item1.Return().Cast<T>().First(), value);
        }

        public virtual T Get<T>(int index)
        {
            if (index == 0)
            {
                return Item1.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode, ToString

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1>;

            return Tuple.StructurallyEquals(this.Item1, rightT.Item1);
        }

        public override int GetHashCode()
        {
            return Item1.GetHashCode();
        }

        public static implicit operator Tuple<T1>(T1 value)
        {
            return new Tuple<T1>(value);
        }

        public override string ToString()
        {
            return string.Format("({0})", ((ITuple)this).GetString());
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2> : Tuple<T1>, ITuple
    {
        public Tuple(T1 item1, T2 item2) : base(item1) { Item2 = item2; }

        public readonly T2 Item2;

        int ITuple.Size
        {
            get
            {
                return 2;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1}", Item1, Item2);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T2) && Equals(Item2, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T2) && comparer.Equals(Item2.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item2, rightT.Item2);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2);
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2, T3> : Tuple<T1, T2>, ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3) : base(item1, item2) { Item3 = item3; }

        public readonly T3 Item3;

        int ITuple.Size
        {
            get
            {
                return 3;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2}", Item1, Item2, Item3);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T3) && Equals(Item3, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T3) && comparer.Equals(Item3.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item3, rightT.Item3);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3);
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2, T3, T4> : Tuple<T1, T2, T3>, ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3, T4 item4) : base(item1, item2, item3) { Item4 = item4; }

        public readonly T4 Item4;

        int ITuple.Size
        {
            get
            {
                return 4;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2},{3}", Item1, Item2, Item3, Item4);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T4) && Equals(Item4, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T4) && comparer.Equals(Item4.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch(index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
                case 3:
                    return Item4.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3, T4>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item4, rightT.Item4);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3, Item4);
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2, T3, T4, T5> : Tuple<T1, T2, T3, T4>, ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5) : base(item1, item2, item3, item4) { Item5 = item5; }

        public readonly T5 Item5;

        int ITuple.Size
        {
            get
            {
                return 5;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2},{3},{4}", Item1, Item2, Item3, Item4, Item5);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T5) && Equals(Item5, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T5) && comparer.Equals(Item5.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
                case 3:
                    return Item4.Return().Cast<T>().First();
                case 4:
                    return Item5.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3, T4, T5>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item5, rightT.Item5);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3, Item4, Item5);
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2, T3, T4, T5, T6> : Tuple<T1, T2, T3, T4, T5>, ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6) : base(item1, item2, item3, item4, item5) { Item6 = item6; }

        public readonly T6 Item6;

        int ITuple.Size
        {
            get
            {
                return 6;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2},{3},{4},{5}", Item1, Item2, Item3, Item4, Item5, Item6);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T6) && Equals(Item6, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T6) && comparer.Equals(Item6.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
                case 3:
                    return Item4.Return().Cast<T>().First();
                case 4:
                    return Item5.Return().Cast<T>().First();
                case 5:
                    return Item6.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3, T4, T5, T6>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item6, rightT.Item6);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3, Item4, Item5, Item6);
        }

        #endregion
    }

    [Serializable]
    public class Tuple<T1, T2, T3, T4, T5, T6, T7> : Tuple<T1, T2, T3, T4, T5, T6>, ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7) : base(item1, item2, item3, item4, item5, item6) { Item7 = item7; }

        public readonly T7 Item7;

        int ITuple.Size
        {
            get
            {
                return 7;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2},{3},{4},{5},{6}", Item1, Item2, Item3, Item4, Item5, Item6, Item7);
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || (typeof(T) == typeof(T7) && Equals(Item7, value));
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || (typeof(T) == typeof(T7) && comparer.Equals(Item7.Return().Cast<T>().First(), value));
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
                case 3:
                    return Item4.Return().Cast<T>().First();
                case 4:
                    return Item5.Return().Cast<T>().First();
                case 5:
                    return Item6.Return().Cast<T>().First();
                case 6:
                    return Item7.Return().Cast<T>().First();
            }

            throw new IndexOutOfRangeException();
        }

        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3, T4, T5, T6, T7>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Item7, rightT.Item7);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3, Item4, Item5, Item6, Item7);
        }

        #endregion
    }

    [Serializable]
    public sealed class Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> : Tuple<T1, T2, T3, T4, T5, T6, T7>, ITuple
        where TRest : ITuple
    {
        public Tuple(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, TRest rest) : base(item1, item2, item3, item4, item5, item6, item7) { Rest = rest; }

        public readonly TRest Rest;
        private int? _restSize;

        int ITuple.Size
        {
            get
            {
                if (!_restSize.HasValue)
                {
                    _restSize = Rest.Size;
                }
                return 7 + _restSize.Value;
            }
        }

        string ITuple.GetString()
        {
            return string.Format("{0},{1},{2},{3},{4},{5},{6},{7}", Item1, Item2, Item3, Item4, Item5, Item6, Item7, Rest.GetString());
        }

        public override bool Contains<T>(T value)
        {
            return base.Contains(value) || Rest.Contains(value);
        }

        public override bool Contains<T>(T value, IEqualityComparer<T> comparer)
        {
            return base.Contains(value, comparer) || Rest.Contains(value, comparer);
        }

        public override T Get<T>(int index)
        {
            switch (index)
            {
                case 0:
                    return Item1.Return().Cast<T>().First();
                case 1:
                    return Item2.Return().Cast<T>().First();
                case 2:
                    return Item3.Return().Cast<T>().First();
                case 3:
                    return Item4.Return().Cast<T>().First();
                case 4:
                    return Item5.Return().Cast<T>().First();
                case 5:
                    return Item6.Return().Cast<T>().First();
                case 6:
                    return Item7.Return().Cast<T>().First();
            }

            if (index > 6 && index < ((ITuple)this).Size)
            {
                return Rest.Get<T>(index - 7);
            }

            throw new IndexOutOfRangeException();
        }
 
        #region Equals, GetHashCode

        public override bool Equals(object right)
        {
            if (right == null)
                return false;

            if (object.ReferenceEquals(this, right))
                return true;

            if (this.GetType() != right.GetType())
                return false;

            var rightT = right as Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>;

            return base.Equals(rightT) && Tuple.StructurallyEquals(this.Rest, rightT.Rest);
        }

        public override int GetHashCode()
        {
            return Tuple.GetHashCode(Item1, Item2, Item3, Item4, Item5, Item6, Item7, Rest);
        }

        #endregion
    }

    public static class Tuple
    {
        public const int MAX_LIST_SIZE = 1024;

        public static ITuple Empty = new EmptyTuple();

        private class EmptyTuple : ITuple
        {
            #region ITuple Members

            public int Size
            {
                get { return 0; }
            }

            public bool Contains<T>(T value)
            {
                return false;
            }

            public bool Contains<T>(T value, IEqualityComparer<T> comparer)
            {
                return false;
            }

            public T Get<T>(int index)
            {
                throw new NotSupportedException();
            }

            public string GetString()
            {
                return string.Empty;
            }

            #endregion

            public override string ToString()
            {
                return string.Format("({0})", ((ITuple)this).GetString());
            }
        }

        public static bool IsEmpty(ITuple tuple)
        {
            return Tuple.Empty.Equals(tuple);
        }

        public static Tuple<T1> Create<T1>(T1 item1)
        {
            return new Tuple<T1>(item1);
        }

        public static Tuple<T1, T2> Create<T1, T2>(T1 item1, T2 item2)
        {
            return new Tuple<T1, T2>(item1, item2);
        }

        public static Tuple<T1, T2, T3> Create<T1, T2, T3>(T1 item1, T2 item2, T3 item3)
        {
            return new Tuple<T1, T2, T3>(item1, item2, item3);
        }

        public static Tuple<T1, T2, T3, T4> Create<T1, T2, T3, T4>(T1 item1, T2 item2, T3 item3, T4 item4)
        {
            return new Tuple<T1, T2, T3, T4>(item1, item2, item3, item4);
        }

        public static Tuple<T1, T2, T3, T4, T5> Create<T1, T2, T3, T4, T5>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5)
        {
            return new Tuple<T1, T2, T3, T4, T5>(item1, item2, item3, item4, item5);
        }

        public static Tuple<T1, T2, T3, T4, T5, T6> Create<T1, T2, T3, T4, T5, T6>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6)
        {
            return new Tuple<T1, T2, T3, T4, T5, T6>(item1, item2, item3, item4, item5, item6);
        }

        public static Tuple<T1, T2, T3, T4, T5, T6, T7> Create<T1, T2, T3, T4, T5, T6, T7>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7)
        {
            return new Tuple<T1, T2, T3, T4, T5, T6, T7>(item1, item2, item3, item4, item5, item6, item7);
        }

        public static Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> Create<T1, T2, T3, T4, T5, T6, T7, TRest>(T1 item1, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7, TRest rest)
            where TRest : ITuple
        {
            return new Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>(item1, item2, item3, item4, item5, item6, item7, rest);
        }

        public static ITuple CreateFromList<T>(IList<T> list)
        {
            return CreateFromList(list, true);
        }

        public static ITuple CreateFromList<T>(IList<T> list, bool silent)
        {
            if (!silent)
            {
                list.ThrowIfNull("list");

                if (list.Count > Tuple.MAX_LIST_SIZE)
                {
                    throw new ArgumentException(string.Format("Can not create a tuple: list size is greater than {0}.", MAX_LIST_SIZE));
                }
            }
            else if (list == null || list.Count > Tuple.MAX_LIST_SIZE)
            {
                return null;
            }
            return CreateFromList(list, 0);
        }

        private static ITuple CreateFromList<T>(IList<T> list, int offset)
        {
            var count = list.Count - offset;
            switch (count)
            {
                case 1:
                    return Tuple.Create(list[0 + offset]);
                case 2:
                    return Tuple.Create(list[0 + offset], list[1 + offset]);
                case 3:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset]);
                case 4:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset], list[3 + offset]);
                case 5:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset], list[3 + offset], list[4 + offset]);
                case 6:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset], list[3 + offset], list[4 + offset], list[5 + offset]);
                case 7:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset], list[3 + offset], list[4 + offset], list[5 + offset], list[6 + offset]);
                default:
                    return Tuple.Create(list[0 + offset], list[1 + offset], list[2 + offset], list[3 + offset], list[4 + offset], list[5 + offset], list[6 + offset], Tuple.CreateFromList(list, 7 + offset));
            }
        }

        internal static bool StructurallyEquals<T>(T left, T right)
        {
            if (left == null && right == null)
                return true;

            if (left == null && right != null)
                return false;

            var len = left as IEnumerable;
            var ren = right as IEnumerable;

            if (len == null && ren == null)
                return left.Equals(right);

            if (len == null && ren != null)
                return false;

            if (len != null && ren == null)
                return false;

            return SequenceEqual(len, ren);
        }

        internal static bool SequenceEqual(IEnumerable en1, IEnumerable en2) 
        {
            var enumerator = en2.GetEnumerator();
            foreach (var o in en1) {

                if (!enumerator.MoveNext())
                    return false;

                if (!StructurallyEquals(o, enumerator.Current))
                    return false;
            }

            return !enumerator.MoveNext();
        }

        internal static int GetHashCode(params object[] args)
        {
            int hash = 17;
            for (int i = 0; i < args.Length; i++)
            {
                hash = hash * 23 + args[i].GetHashCode();
            }
            return hash;

        }
    }
}