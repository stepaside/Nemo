using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.Linq;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Nemo.UnitTests
{
    [TestClass]
    public class NemoQueryParserTests
    {
        public class TestEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
            public bool IsActive { get; set; }
            public decimal Amount { get; set; }
        }

        private static IQueryable<TestEntity> Query => new NemoQueryable<TestEntity>();

        private static NemoQueryPlan Parse(IQueryable queryable)
        {
            return NemoQueryParser.Parse(queryable.Expression, false);
        }

        private static NemoQueryPlan Parse(Expression expression)
        {
            return NemoQueryParser.Parse(expression, false);
        }

        [TestMethod]
        public void Parse_SkipAndTake_CapturedInPlan()
        {
            var plan = Parse(Query.OrderBy(x => x.Id).Skip(10).Take(5));

            Assert.AreEqual(10, plan.Skip);
            Assert.AreEqual(5, plan.Take);

            plan.GetPaging(out var page, out var pageSize, out var skipCount);
            Assert.AreEqual(3, page);
            Assert.AreEqual(5, pageSize);
            Assert.AreEqual(0, skipCount);
        }

        [TestMethod]
        public void Parse_SkipAndTake_CapturedVariables()
        {
            var skip = 10;
            var take = 5;
            var plan = Parse(Query.OrderBy(x => x.Id).Skip(skip).Take(take));

            Assert.AreEqual(10, plan.Skip);
            Assert.AreEqual(5, plan.Take);
        }

        [TestMethod]
        public void CreateQuery_NonGeneric_ReturnsTypedQueryable()
        {
            var provider = new NemoQueryProvider();
            var source = Query.Where(x => x.Id > 0);

            var queryable = ((IQueryProvider)provider).CreateQuery(source.Expression);

            Assert.IsInstanceOfType(queryable, typeof(NemoQueryable<TestEntity>));
            Assert.AreSame(source.Expression, queryable.Expression);
        }

        [TestMethod]
        public void Parse_SkipOnly_MapsToSkipCount()
        {
            var plan = Parse(Query.Skip(7));

            plan.GetPaging(out var page, out var pageSize, out var skipCount);
            Assert.AreEqual(0, page);
            Assert.AreEqual(0, pageSize);
            Assert.AreEqual(7, skipCount);
        }

        [TestMethod]
        public void Parse_NonAlignedSkipTake_Throws()
        {
            var plan = Parse(Query.Skip(5).Take(10));

            Assert.ThrowsException<NotSupportedException>(() => plan.GetPaging(out _, out _, out _));
        }

        [TestMethod]
        public void Parse_TakeThenSkip_Throws()
        {
            Assert.ThrowsException<NotSupportedException>(() => Parse(Query.Take(10).Skip(5)));
        }

        [TestMethod]
        public void Parse_WhereAfterTake_Throws()
        {
            Assert.ThrowsException<NotSupportedException>(() => Parse(Query.Take(10).Where(x => x.IsActive)));
        }

        [TestMethod]
        public void Parse_MultipleWhere_CombinesPredicates()
        {
            var plan = Parse(Query.Where(x => x.Id > 1).Where(x => x.IsActive));

            Assert.IsNotNull(plan.Predicate);
            var predicate = (Func<TestEntity, bool>)plan.Predicate.Compile();
            Assert.IsTrue(predicate(new TestEntity { Id = 2, IsActive = true }));
            Assert.IsFalse(predicate(new TestEntity { Id = 2, IsActive = false }));
            Assert.IsFalse(predicate(new TestEntity { Id = 1, IsActive = true }));
        }

        [TestMethod]
        public void Parse_OrderByThenBy_PreservesAllKeys()
        {
            var plan = Parse(Query.OrderBy(x => x.Name).ThenByDescending(x => x.Id));

            Assert.AreEqual(2, plan.OrderBy.Count);
            Assert.IsFalse(plan.OrderBy[0].Descending);
            Assert.IsTrue(plan.OrderBy[1].Descending);
        }

        [TestMethod]
        public void Parse_OrderByOnSource_Captured()
        {
            var plan = Parse(Query.OrderByDescending(x => x.Id));

            Assert.AreEqual(1, plan.OrderBy.Count);
            Assert.IsTrue(plan.OrderBy[0].Descending);
        }

        [TestMethod]
        public void Parse_SecondOrderBy_ResetsOrdering()
        {
            var plan = Parse(Query.OrderBy(x => x.Name).OrderBy(x => x.Id));

            Assert.AreEqual(1, plan.OrderBy.Count);
        }

        [TestMethod]
        public void Parse_CountWithoutPredicate_Works()
        {
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(TestEntity) }, Query.Expression);

            var plan = Parse(expression);

            Assert.IsTrue(plan.IsCount);
            Assert.IsFalse(plan.IsLongCount);
            Assert.IsNull(plan.Predicate);
        }

        [TestMethod]
        public void Parse_CountWithPredicateOnWhere_CombinesPredicates()
        {
            Expression<Func<TestEntity, bool>> countPredicate = x => x.IsActive;
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(TestEntity) },
                Query.Where(x => x.Id > 1).Expression, Expression.Quote(countPredicate));

            var plan = Parse(expression);

            Assert.IsTrue(plan.IsCount);
            var predicate = (Func<TestEntity, bool>)plan.Predicate.Compile();
            Assert.IsTrue(predicate(new TestEntity { Id = 2, IsActive = true }));
            Assert.IsFalse(predicate(new TestEntity { Id = 1, IsActive = true }));
        }

        [TestMethod]
        public void Parse_FirstWithPredicate_SetsSelectOptionAndPredicate()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1;
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(TestEntity) },
                Query.Expression, Expression.Quote(predicate));

            var plan = Parse(expression);

            Assert.AreEqual(SelectOption.First, plan.SelectOption);
            Assert.IsNotNull(plan.Predicate);
        }

        [TestMethod]
        public void Parse_FirstOrDefaultWithoutPredicate_SetsSelectOption()
        {
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.FirstOrDefault), new[] { typeof(TestEntity) }, Query.Expression);

            var plan = Parse(expression);

            Assert.AreEqual(SelectOption.FirstOrDefault, plan.SelectOption);
        }

        [TestMethod]
        public void Parse_MaxWithPropertySelector_CapturesAggregate()
        {
            Expression<Func<TestEntity, int>> selector = x => x.Id;
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.Max), new[] { typeof(TestEntity), typeof(int) },
                Query.Expression, Expression.Quote(selector));

            var plan = Parse(expression);

            Assert.AreEqual(ObjectFactory.AggregateNames.MAX, plan.Aggregate);
            Assert.AreEqual(nameof(TestEntity.Id), plan.AggregateProperty.Name);
        }

        [TestMethod]
        public void Parse_Average_MapsToAvg()
        {
            Expression<Func<TestEntity, decimal>> selector = x => x.Amount;
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.Average), new[] { typeof(TestEntity) },
                Query.Expression, Expression.Quote(selector));

            var plan = Parse(expression);

            Assert.AreEqual(ObjectFactory.AggregateNames.AVG, plan.Aggregate);
        }

        [TestMethod]
        public void Parse_SumWithComputedProjection_Throws()
        {
            Expression<Func<TestEntity, decimal>> selector = x => x.Amount * 2;
            var expression = Expression.Call(typeof(Queryable), nameof(Queryable.Sum), new[] { typeof(TestEntity) },
                Query.Expression, Expression.Quote(selector));

            Assert.ThrowsException<NotSupportedException>(() => Parse(expression));
        }

        [TestMethod]
        public void Parse_SelectProjection_Throws()
        {
            Assert.ThrowsException<NotSupportedException>(() => Parse(Query.Select(x => new { x.Id })));
        }

        [TestMethod]
        public void Parse_Distinct_Throws()
        {
            Assert.ThrowsException<NotSupportedException>(() => Parse(Query.Distinct()));
        }

        [TestMethod]
        public void Parse_GroupBy_Throws()
        {
            Assert.ThrowsException<NotSupportedException>(() => Parse(Query.GroupBy(x => x.Name)));
        }

        [TestMethod]
        public void Parse_TakeWithCapturedVariable_Evaluates()
        {
            var count = 4;
            var plan = Parse(Query.Take(count));

            Assert.AreEqual(4, plan.Take);
        }

        [TestMethod]
        public void Parse_ToListAsync_TreatedAsNoOp()
        {
            var source = new NemoQueryableAsync<TestEntity>();
            var expression = Expression.Call(typeof(AsyncQueryable), "ToListAsync", new[] { typeof(TestEntity) },
                source.Expression, Expression.Constant(default(System.Threading.CancellationToken)));

            var plan = NemoQueryParser.Parse(expression, true);

            Assert.IsFalse(plan.IsCount);
            Assert.IsNull(plan.Aggregate);
            Assert.AreEqual(Nemo.SelectOption.All, plan.SelectOption);
        }

        [TestMethod]
        public void Parse_ToArrayAsync_TreatedAsNoOp()
        {
            var source = new NemoQueryableAsync<TestEntity>();
            var expression = Expression.Call(typeof(AsyncQueryable), "ToArrayAsync", new[] { typeof(TestEntity) },
                source.Expression, Expression.Constant(default(System.Threading.CancellationToken)));

            var plan = NemoQueryParser.Parse(expression, true);

            Assert.IsFalse(plan.IsCount);
            Assert.IsNull(plan.Aggregate);
            Assert.AreEqual(Nemo.SelectOption.All, plan.SelectOption);
        }

        [TestMethod]
        public void Parse_ValueTypeOrderByKey_Preserved()
        {
            var plan = Parse(Query.OrderBy(x => x.Id));

            Assert.AreEqual(1, plan.OrderBy.Count);
            Assert.AreEqual(typeof(int), plan.OrderBy[0].KeySelector.Body.Type);
        }
    }
}
