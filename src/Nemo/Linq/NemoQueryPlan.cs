using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Nemo.Linq
{
    internal sealed class NemoQuerySort
    {
        public LambdaExpression KeySelector { get; set; }
        public bool Descending { get; set; }
    }

    internal sealed class NemoQueryPlan
    {
        public Type ElementType { get; set; }
        public LambdaExpression Predicate { get; set; }
        public List<NemoQuerySort> OrderBy { get; } = new List<NemoQuerySort>();
        public int Skip { get; set; }
        public int Take { get; set; }
        public SelectOption SelectOption { get; set; } = SelectOption.All;
        public bool IsCount { get; set; }
        public bool IsLongCount { get; set; }
        public ObjectFactory.AggregateNames? Aggregate { get; set; }
        public LambdaExpression AggregateProjection { get; set; }
        public PropertyInfo AggregateProperty { get; set; }

        public void GetPaging(out int page, out int pageSize, out int skipCount)
        {
            page = 0;
            pageSize = 0;
            skipCount = 0;

            if (Take > 0 && Skip > 0)
            {
                if (Skip % Take != 0)
                {
                    throw new NotSupportedException($"Skip({Skip}) combined with Take({Take}) is not supported unless Skip is a multiple of Take (page-aligned access).");
                }
                page = Skip / Take + 1;
                pageSize = Take;
            }
            else if (Take > 0)
            {
                page = 1;
                pageSize = Take;
            }
            else if (Skip > 0)
            {
                skipCount = Skip;
            }
        }
    }

    internal static class NemoQueryParser
    {
        public static NemoQueryPlan Parse(Expression expression, bool async)
        {
            var calls = new List<MethodCallExpression>();
            var current = expression;
            while (current is MethodCallExpression call)
            {
                calls.Add(call);
                if (call.Arguments.Count == 0)
                {
                    throw new NotSupportedException($"The LINQ operator '{call.Method.Name}' is not supported by the Nemo LINQ provider.");
                }
                current = call.Arguments[0];
            }

            if (!(current is ConstantExpression source))
            {
                throw new NotSupportedException("Invalid query: expected a Nemo query source.");
            }

            var plan = new NemoQueryPlan { ElementType = GetElementType(source) };

            // Process in application order (innermost call first)
            for (var i = calls.Count - 1; i >= 0; i--)
            {
                Apply(plan, calls[i], async);
            }

            return plan;
        }

        private static Type GetElementType(ConstantExpression source)
        {
            switch (source.Value)
            {
                case IQueryable queryable:
                    return queryable.ElementType;
                case IAsyncQueryable asyncQueryable:
                    return asyncQueryable.ElementType;
                default:
                    throw new NotSupportedException("Invalid query: expected a Nemo query source.");
            }
        }

        private static void Apply(NemoQueryPlan plan, MethodCallExpression call, bool async)
        {
            var method = call.Method;
            if (method.DeclaringType != typeof(Queryable) && method.DeclaringType != typeof(AsyncQueryable))
            {
                throw new NotSupportedException($"The method '{method.DeclaringType}.{method.Name}' is not supported by the Nemo LINQ provider.");
            }

            var name = method.Name;
            if (name.EndsWith("Async", StringComparison.Ordinal))
            {
                name = name.Substring(0, name.Length - 5);
            }

            switch (name)
            {
                case "Where":
                    EnsureNoPaging(plan, name);
                    AddPredicate(plan, GetRequiredLambda(call));
                    break;

                case "OrderBy":
                case "OrderByDescending":
                    EnsureNoPaging(plan, name);
                    plan.OrderBy.Clear();
                    plan.OrderBy.Add(new NemoQuerySort { KeySelector = GetRequiredLambda(call), Descending = name == "OrderByDescending" });
                    break;

                case "ThenBy":
                case "ThenByDescending":
                    EnsureNoPaging(plan, name);
                    plan.OrderBy.Add(new NemoQuerySort { KeySelector = GetRequiredLambda(call), Descending = name == "ThenByDescending" });
                    break;

                case "Skip":
                    if (plan.Take > 0)
                    {
                        throw new NotSupportedException("Skip after Take is not supported by the Nemo LINQ provider; apply Skip before Take.");
                    }
                    plan.Skip += GetInt(call.Arguments[1]);
                    break;

                case "Take":
                    var take = GetInt(call.Arguments[1]);
                    plan.Take = plan.Take > 0 ? Math.Min(plan.Take, take) : take;
                    break;

                case "First":
                case "FirstOrDefault":
                    plan.SelectOption = name == "First" ? SelectOption.First : SelectOption.FirstOrDefault;
                    var firstPredicate = GetOptionalLambda(call);
                    if (firstPredicate != null)
                    {
                        EnsureNoPaging(plan, name);
                        AddPredicate(plan, firstPredicate);
                    }
                    break;

                case "ToList":
                case "ToArray":
                    break;

                case "Count":
                case "LongCount":
                    EnsureNoPaging(plan, name);
                    plan.IsCount = true;
                    plan.IsLongCount = name == "LongCount";
                    var countPredicate = GetOptionalLambda(call);
                    if (countPredicate != null)
                    {
                        AddPredicate(plan, countPredicate);
                    }
                    break;

                case "Max":
                case "Min":
                case "Sum":
                case "Average":
                    EnsureNoPaging(plan, name);
                    var projection = GetOptionalLambda(call);
                    if (projection == null)
                    {
                        throw new NotSupportedException($"{name} requires a property selector, e.g. query.{name}(x => x.Property).");
                    }
                    if (!(Unquote(projection.Body) is MemberExpression member) || !(member.Member is PropertyInfo property))
                    {
                        throw new NotSupportedException($"{name} supports only simple property selectors, e.g. query.{name}(x => x.Property).");
                    }
                    plan.Aggregate = GetAggregateName(name);
                    plan.AggregateProperty = property;
                    plan.AggregateProjection = Expression.Lambda(typeof(Func<,>).MakeGenericType(plan.ElementType, property.PropertyType), member, projection.Parameters);
                    break;

                default:
                    throw new NotSupportedException($"The LINQ operator '{method.Name}' is not supported by the Nemo LINQ provider.");
            }
        }

        private static ObjectFactory.AggregateNames GetAggregateName(string name)
        {
            switch (name)
            {
                case "Max": return ObjectFactory.AggregateNames.MAX;
                case "Min": return ObjectFactory.AggregateNames.MIN;
                case "Sum": return ObjectFactory.AggregateNames.SUM;
                default: return ObjectFactory.AggregateNames.AVG;
            }
        }

        private static void EnsureNoPaging(NemoQueryPlan plan, string operatorName)
        {
            if (plan.Skip > 0 || plan.Take > 0)
            {
                throw new NotSupportedException($"'{operatorName}' after Skip or Take is not supported by the Nemo LINQ provider.");
            }
        }

        private static void AddPredicate(NemoQueryPlan plan, LambdaExpression predicate)
        {
            if (predicate.Parameters.Count != 1)
            {
                throw new NotSupportedException("Indexed predicates are not supported by the Nemo LINQ provider.");
            }

            if (plan.Predicate == null)
            {
                plan.Predicate = predicate;
                return;
            }

            var parameter = plan.Predicate.Parameters[0];
            var body = new ParameterReplacer(predicate.Parameters[0], parameter).Visit(predicate.Body);
            plan.Predicate = Expression.Lambda(plan.Predicate.Type, Expression.AndAlso(plan.Predicate.Body, body), parameter);
        }

        private static LambdaExpression GetRequiredLambda(MethodCallExpression call)
        {
            var lambda = GetOptionalLambda(call);
            if (lambda == null)
            {
                throw new NotSupportedException($"The LINQ operator '{call.Method.Name}' requires a lambda expression argument.");
            }
            return lambda;
        }

        private static LambdaExpression GetOptionalLambda(MethodCallExpression call)
        {
            for (var i = 1; i < call.Arguments.Count; i++)
            {
                var lambda = AsLambda(call.Arguments[i]);
                if (lambda != null)
                {
                    return lambda;
                }
            }
            return null;
        }

        private static LambdaExpression AsLambda(Expression argument)
        {
            var expression = Unquote(argument);
            switch (expression)
            {
                case LambdaExpression lambda:
                    return lambda;
                case ConstantExpression constant when constant.Value is LambdaExpression value:
                    return value;
                default:
                    return null;
            }
        }

        private static Expression Unquote(Expression expression)
        {
            while (expression is UnaryExpression unary && (unary.NodeType == ExpressionType.Quote || unary.NodeType == ExpressionType.Convert))
            {
                expression = unary.Operand;
            }
            return expression;
        }

        private static int GetInt(Expression argument)
        {
            if (argument is ConstantExpression constant)
            {
                return Convert.ToInt32(constant.Value);
            }
            return Convert.ToInt32(Expression.Lambda(argument).Compile().DynamicInvoke());
        }

        private class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression _source;
            private readonly ParameterExpression _target;

            public ParameterReplacer(ParameterExpression source, ParameterExpression target)
            {
                _source = source;
                _target = target;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == _source ? _target : base.VisitParameter(node);
            }
        }
    }
}
