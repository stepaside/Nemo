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
            Assert.AreEqual(0, page);
            Assert.AreEqual(5, pageSize);
            Assert.AreEqual(10, skipCount);
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
        public void Parse_NonAlignedSkipTake_MapsToSkipCountAndPageSize()
        {
            var plan = Parse(Query.Skip(5).Take(10));

            plan.GetPaging(out var page, out var pageSize, out var skipCount);
            Assert.AreEqual(0, page);
            Assert.AreEqual(10, pageSize);
            Assert.AreEqual(5, skipCount);
        }

        [TestMethod]
        public void Parse_TakeThenSkip_RewritesToSkipThenTake()
        {
            var plan = Parse(Query.Take(10).Skip(3));

            Assert.AreEqual(3, plan.Skip);
            Assert.AreEqual(7, plan.Take);
            Assert.IsFalse(plan.IsEmpty);
        }

        [TestMethod]
        public void Parse_TakeThenSkipAll_IsEmpty()
        {
            var plan = Parse(Query.Take(10).Skip(10));

            Assert.IsTrue(plan.IsEmpty);
        }

        [TestMethod]
        public void Parse_TakeZero_IsEmpty()
        {
            var plan = Parse(Query.Take(0));

            Assert.IsTrue(plan.IsEmpty);
        }

        [TestMethod]
        public void Parse_TakeSkipTake_ComposesCorrectly()
        {
            var plan = Parse(Query.Take(10).Skip(5).Take(3));

            Assert.AreEqual(5, plan.Skip);
            Assert.AreEqual(3, plan.Take);
            Assert.IsFalse(plan.IsEmpty);
        }

        [TestMethod]
        public void Parse_OrderByAfterPaging_CapturedAsPostOrderBy()
        {
            var plan = Parse(Query.OrderBy(x => x.Id).Skip(3).Take(7).OrderBy(x => x.Name).ThenByDescending(x => x.Id));

            Assert.AreEqual(1, plan.OrderBy.Count);
            Assert.AreEqual(2, plan.PostOrderBy.Count);
            Assert.IsFalse(plan.PostOrderBy[0].Descending);
            Assert.IsTrue(plan.PostOrderBy[1].Descending);
            Assert.AreEqual(3, plan.Skip);
            Assert.AreEqual(7, plan.Take);
        }

        [TestMethod]
        public void Parse_SkipAfterPostPagingOrderBy_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.Take(10).OrderBy(x => x.Name).Skip(2)));
        }

        [TestMethod]
        public void Parse_TakeAfterPostPagingOrderBy_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.Take(10).OrderBy(x => x.Name).Take(2)));
        }

        [TestMethod]
        public void Parse_WhereAfterTake_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.Take(10).Where(x => x.IsActive)));
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

            Assert.Throws<NotSupportedException>(() => Parse(expression));
        }

        [TestMethod]
        public void Parse_SelectProjection_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.Select(x => new { x.Id })));
        }

        [TestMethod]
        public void Parse_Distinct_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.Distinct()));
        }

        [TestMethod]
        public void Parse_GroupBy_Throws()
        {
            Assert.Throws<NotSupportedException>(() => Parse(Query.GroupBy(x => x.Name)));
        }

        [TestMethod]
        public void Parse_TakeWithCapturedVariable_Evaluates()
        {
            var count = 4;
            var plan = Parse(Query.Take(count));

            Assert.AreEqual(4, plan.Take);
        }

        [TestMethod]
        public void Parse_FirstOrDefaultAsyncWithPredicate_ProducesFirstOrDefaultPlan()
        {
            var expression = Expression.Call(typeof(NemoQueryableExtensions), nameof(NemoQueryableExtensions.FirstOrDefaultAsync), new[] { typeof(TestEntity) },
                Query.Expression, Expression.Quote((Expression<Func<TestEntity, bool>>)(x => x.Name == "test")), Expression.Constant(default(System.Threading.CancellationToken)));

            var plan = NemoQueryParser.Parse(expression, true);

            Assert.AreEqual(Nemo.SelectOption.FirstOrDefault, plan.SelectOption);
            Assert.IsNotNull(plan.Predicate);
        }

        [TestMethod]
        public void Parse_CountAsync_ProducesCountPlan()
        {
            var expression = Expression.Call(typeof(NemoQueryableExtensions), nameof(NemoQueryableExtensions.CountAsync), new[] { typeof(TestEntity) },
                Query.Expression, Expression.Constant(default(System.Threading.CancellationToken)));

            var plan = NemoQueryParser.Parse(expression, true);

            Assert.IsTrue(plan.IsCount);
            Assert.IsFalse(plan.IsLongCount);
        }

        [TestMethod]
        public void Parse_MaxAsync_ProducesAggregatePlan()
        {
            var expression = Expression.Call(typeof(NemoQueryableExtensions), nameof(NemoQueryableExtensions.MaxAsync), new[] { typeof(TestEntity), typeof(int) },
                Query.Expression, Expression.Quote((Expression<Func<TestEntity, int>>)(x => x.Id)), Expression.Constant(default(System.Threading.CancellationToken)));

            var plan = NemoQueryParser.Parse(expression, true);

            Assert.AreEqual(ObjectFactory.AggregateNames.MAX, plan.Aggregate);
            Assert.AreEqual("Id", plan.AggregateProperty.Name);
        }

        [TestMethod]
        public async System.Threading.Tasks.Task ToListAsync_NonNemoQueryable_Throws()
        {
            var query = new[] { new TestEntity() }.AsQueryable();

            await Assert.ThrowsAsync<InvalidOperationException>(() => query.ToListAsync());
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
