/**
 * This is based on ExpressionVisitor implementation from ServiceStack.OrmLite
 * https://github.com/ServiceStack/ServiceStack.OrmLite
 * **/

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Nemo.Extensions;
using System.Reflection;
using Nemo.Reflection;
using Nemo.Attributes;

namespace Nemo.Data
{
    internal static class ExpressionVisitor
    {
        internal static string Visit<T>(Expression exp, DialectProvider dialect)
        {
            if (exp == null) return string.Empty;
            switch (exp.NodeType)
            {
                case ExpressionType.Lambda:
                    return VisitLambda<T>(exp as LambdaExpression, dialect);
                case ExpressionType.MemberAccess:
                    return VisitMemberAccess<T>(exp as MemberExpression, dialect);
                case ExpressionType.Constant:
                    return VisitConstant(exp as ConstantExpression, dialect);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    return "(" + VisitBinary<T>(exp as BinaryExpression, dialect) + ")";
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    return VisitUnary<T>(exp as UnaryExpression, dialect);
                case ExpressionType.Parameter:
                    return VisitParameter(exp as ParameterExpression, dialect);
                case ExpressionType.Call:
                    return VisitMethodCall<T>(exp as MethodCallExpression, dialect);
                case ExpressionType.New:
                    return VisitNew<T>(exp as NewExpression, dialect);
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    return VisitNewArray<T>(exp as NewArrayExpression, dialect);
                default:
                    return exp.ToString();
            }
        }

        private static string VisitLambda<T>(LambdaExpression lambda, DialectProvider dialect)
        {
            if (lambda.Body.NodeType == ExpressionType.MemberAccess)
            {
                MemberExpression m = lambda.Body as MemberExpression;

                if (m.Expression != null)
                {
                    string r = VisitMemberAccess<T>(m, dialect);
                    return string.Format("{0}={1}", r, GetQuotedTrueValue());
                }

            }
            return Visit<T>(lambda.Body, dialect);
        }

        private static string VisitBinary<T>(BinaryExpression b, DialectProvider dialect)
        {
            string left, right;
            var operand = BindOperant(b.NodeType);
            if (operand == "AND" || operand == "OR")
            {
                MemberExpression m = b.Left as MemberExpression;
                if (m != null && m.Expression != null)
                {
                    string r = VisitMemberAccess<T>(m, dialect);
                    left = string.Format("{0}={1}", r, GetQuotedTrueValue());
                }
                else
                {
                    left = Visit<T>(b.Left, dialect);
                }
                m = b.Right as MemberExpression;
                if (m != null && m.Expression != null)
                {
                    string r = VisitMemberAccess<T>(m, dialect);
                    right = string.Format("{0}={1}", r, GetQuotedTrueValue());
                }
                else
                {
                    right = Visit<T>(b.Right, dialect);
                }
            }
            else
            {
                left = Visit<T>(b.Left, dialect);
                right = Visit<T>(b.Right, dialect);
            }

            if (operand == "=" && right == "null") operand = "is";
            else if (operand == "<>" && right == "null") operand = "is not";
            else if (operand == "=" || operand == "<>")
            {
                if (IsTrueExpression(right)) right = GetQuotedTrueValue();
                else if (IsFalseExpression(right)) right = GetQuotedFalseValue();

                if (IsTrueExpression(left)) left = GetQuotedTrueValue();
                else if (IsFalseExpression(left)) left = GetQuotedFalseValue();

            }

            switch (operand)
            {
                case "MOD":
                case "COALESCE":
                    return string.Format("{0}({1},{2})", operand, left, right);
                default:
                    return left + " " + operand + " " + right;
            }
        }

        private static string VisitMemberAccess<T>(MemberExpression m, DialectProvider dialect)
        {
            Type elementType = null;
            if (m.Expression != null && m.Expression.Type == typeof(T))
            {
                //return m.Member.Name;
                return MapColumnAttribute.GetMappedColumnName((PropertyInfo)m.Member);
            }
            else if (m.Expression != null && Reflector.IsDataEntityList(m.Expression.Type, out elementType) && m.Member.Name == "Count")
            {
                var parentTable = ObjectFactory.GetTableName(typeof(T));
                var childTable = ObjectFactory.GetTableName(elementType);
                
                var parentPropertyMap = Reflector.GetPropertyMap(typeof(T));
                var whereClause = parentPropertyMap.Where(p => p.Value.IsPrimaryKey).Select(p => string.Format("{0}{3}{1}.{0}{2}{1} = {0}{4}{1}.{0}{2}{1}", dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter, p.Value.MappedColumnName, parentTable, childTable)).ToDelimitedString(" AND ");

                return string.Format("(SELECT COUNT(*) FROM {0}{1}{2} WHERE {3})", dialect.IdentifierEscapeStartCharacter, childTable, dialect.IdentifierEscapeEndCharacter, whereClause);
            }
            else
            {
                var member = Expression.Convert(m, typeof(object));
                var lambda = Expression.Lambda<Func<object>>(member);
                var getter = lambda.Compile();
                object o = getter();
                return GetQuotedValue(o, o.GetType());
            }
        }

        private static string VisitNew<T>(NewExpression nex, DialectProvider dialect)
        {
            // TODO : check !
            var member = Expression.Convert(nex, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(member);
            try
            {
                var getter = lambda.Compile();
                object o = getter();
                return GetQuotedValue(o, o.GetType());
            }
            catch (System.InvalidOperationException)
            { // FieldName ?
                List<Object> exprs = VisitExpressionList<T>(nex.Arguments, dialect);
                StringBuilder r = new StringBuilder();
                foreach (Object e in exprs)
                {
                    r.AppendFormat("{0}{1}", r.Length > 0 ? "," : "", e);
                }
                return r.ToString();
            }

        }

        private static string VisitParameter(ParameterExpression p, DialectProvider dialect)
        {
            return p.Name;
        }

        private static string VisitConstant(ConstantExpression c, DialectProvider dialect)
        {
            if (c.Value == null)
                return "null";
            else if (c.Value.GetType() == typeof(bool))
            {
                object o = GetQuotedValue(c.Value, c.Value.GetType());
                return string.Format("({0}={1})", GetQuotedTrueValue(), o);
            }
            else
                return GetQuotedValue(c.Value, c.Value.GetType());
        }

        private static string VisitUnary<T>(UnaryExpression u, DialectProvider dialect)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    string o = Visit<T>(u.Operand, dialect);
                    return "NOT (" + o + ")";
                default:
                    return Visit<T>(u.Operand, dialect);
            }
        }

        private static string VisitMethodCall<T>(MethodCallExpression m, DialectProvider dialect)
        {
            List<Object> args = VisitExpressionList<T>(m.Arguments, dialect);

            Object r;
            if (m.Object != null)
                r = Visit<T>(m.Object, dialect);
            else
            {
                r = args[0];
                args.RemoveAt(0);
            }

            switch (m.Method.Name)
            {
                case "ToUpper":
                    return string.Format("upper({0})", r);
                case "ToLower":
                    return string.Format("lower({0})", r);
                case "StartsWith":
                    return string.Format("upper({0}) like '{1}%' ", r, RemoveQuote(args[0].ToString()).ToUpper());
                case "EndsWith":
                    return string.Format("upper({0}) like '%{1}'", r, RemoveQuote(args[0].ToString()).ToUpper());
                case "Contains":
                    return string.Format("upper({0}) like '%{1}%'", r, RemoveQuote(args[0].ToString()).ToUpper());
                case "Substring":
                    var startIndex = Int32.Parse(args[0].ToString()) + 1;
                    if (args.Count == 2)
                    {
                        var length = Int32.Parse(args[1].ToString());
                        return string.Format(dialect.SubstringFunction + "({0}, {1}, {2})",
                        r,
                        startIndex,
                        length);
                    }
                    else
                        return string.Format(dialect.SubstringFunction + "({0}, {1})",
                        r,
                        startIndex);
                case "Round":
                case "Floor":
                case "Ceiling":
                case "Coalesce":
                case "Abs":
                case "Sum":
                    return string.Format("{0}({1}{2})",
                    m.Method.Name,
                    r,
                    args.Count == 1 ? string.Format(",{0}", args[0]) : "");
                case "Concat":
                    if (dialect.StringConcatenationFunction != null)
                    {
                        StringBuilder s = new StringBuilder();
                        s.Append(dialect.StringConcatenationFunction);
                        s.Append("(");
                        for (var i = 0; i < args.Count; i++)
                        {
                            s.AppendFormat("{0}", args[i]);
                            if (i != args.Count - 1)
                            {
                                s.AppendFormat(",");
                            }
                        }
                        s.Append(")");
                        return string.Format("{0}{1}", r, s.ToString());

                    }
                    else
                    {
                        StringBuilder s = new StringBuilder();
                        foreach (var e in args)
                        {
                            s.AppendFormat(" {0} {1}", dialect.StringConcatenationOperator, e);
                        }
                        return string.Format("{0}{1}", r, s.ToString());
                    }
                case "In":

                    var member = Expression.Convert(m.Arguments[1], typeof(object));
                    var lambda = Expression.Lambda<Func<object>>(member);
                    var getter = lambda.Compile();

                    var inArgs = getter() as object[];

                    StringBuilder sIn = new StringBuilder();
                    foreach (var e in inArgs)
                    {
                        if (e.GetType().ToString() != "System.Collections.Generic.List`1[System.Object]")
                        {
                            sIn.AppendFormat("{0}{1}", sIn.Length > 0 ? "," : "", GetQuotedValue(e, e.GetType()));
                        }
                        else
                        {
                            var listArgs = e as IList<object>;
                            foreach (var el in listArgs)
                            {
                                sIn.AppendFormat("{0}{1}", sIn.Length > 0 ? "," : "", GetQuotedValue(el, el.GetType()));
                            }
                        }
                    }

                    return string.Format("{0} {1} ({2})", r, m.Method.Name, sIn.ToString());
                case "Desc":
                    return string.Format("{0} DESC", r);
                case "As":
                    return string.Format("{0} As [{1}]", r, RemoveQuote(args[0].ToString()));
                case "ToString":
                    return r.ToString();
                default:
                    StringBuilder s2 = new StringBuilder();
                    foreach (var e in args)
                    {
                        s2.AppendFormat(",{0}", GetQuotedValue(e, e.GetType()));
                    }
                    return string.Format("{0}({1}{2})", m.Method.Name, r, s2.ToString());
            }
        }

        private static List<Object> VisitExpressionList<T>(ReadOnlyCollection<Expression> original, DialectProvider dialect)
        {
            List<Object> list = new List<Object>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                if (original[i].NodeType == ExpressionType.NewArrayInit ||
                                 original[i].NodeType == ExpressionType.NewArrayBounds)
                {

                    list.AddRange(VisitNewArrayFromExpressionList<T>(original[i] as NewArrayExpression, dialect));
                }
                else
                    list.Add(Visit<T>(original[i], dialect));

            }
            return list;
        }

        private static string VisitNewArray<T>(NewArrayExpression na, DialectProvider dialect)
        {
            List<Object> exprs = VisitExpressionList<T>(na.Expressions, dialect);
            StringBuilder r = new StringBuilder();
            foreach (Object e in exprs)
            {
                r.Append(r.Length > 0 ? "," + e : e);
            }

            return r.ToString();
        }

        private static List<Object> VisitNewArrayFromExpressionList<T>(NewArrayExpression na, DialectProvider dialect)
        {
            List<Object> exprs = VisitExpressionList<T>(na.Expressions, dialect);
            return exprs;
        }

        private static string BindOperant(ExpressionType e)
        {
            switch (e)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                case ExpressionType.Add:
                    return "+";
                case ExpressionType.Subtract:
                    return "-";
                case ExpressionType.Multiply:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "MOD";
                case ExpressionType.Coalesce:
                    return "COALESCE";
                default:
                    return e.ToString();
            }
        }

        private static string RemoveQuote(string exp)
        {
            if (exp.StartsWith("'"))
                exp = exp.Remove(0, 1);
            if (exp.EndsWith("'"))
                exp = exp.Remove(exp.Length - 1, 1);
            return exp;
        }

        private static string GetTrueExpression()
        {
            object o = GetQuotedTrueValue();
            return string.Format("({0}={1})", o, o);
        }

        private static string GetFalseExpression()
        {
            return string.Format("({0}={1})",
            GetQuotedTrueValue(),
            GetQuotedFalseValue());
        }

        private static bool IsTrueExpression(string exp)
        {
            return (exp == GetTrueExpression());
        }

        private static bool IsFalseExpression(string exp)
        {
            return (exp == GetFalseExpression());
        }

        private static string GetQuotedTrueValue()
        {
            return GetQuotedValue(true, typeof(bool));
        }

        private static string GetQuotedFalseValue()
        {
            return GetQuotedValue(false, typeof(bool));
        }

        private static string GetQuotedValue(object value, Type fieldType)
        {
            if (value == null) return "NULL";

            if (!fieldType.UnderlyingSystemType.IsValueType && fieldType != typeof(string))
            {
                return "'" + EscapeParam(value.SafeCast<string>()) + "'";
            }

            if (fieldType == typeof(float))
                return ((float)value).ToString(CultureInfo.InvariantCulture);

            if (fieldType == typeof(double))
                return ((double)value).ToString(CultureInfo.InvariantCulture);

            if (fieldType == typeof(decimal))
                return ((decimal)value).ToString(CultureInfo.InvariantCulture);

            return !Reflector.IsNumeric(fieldType) ? "'" + EscapeParam(value) + "'" : value.ToString();
        }

        private static string EscapeParam(object paramValue)
        {
            return paramValue.ToString().Replace("'", "''");
        }
    }
}
