package io.basestar.schema.from;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.With;
import io.basestar.expression.sql.Select;
import io.basestar.expression.sql.Sql;
import io.basestar.expression.sql.Union;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FromSqlVisitor implements ExpressionVisitor.Defaulting<From> {

    private final Schema.Resolver.Constructing resolver;

    private final Map<String, From> using;

    public FromSqlVisitor(final Schema.Resolver.Constructing resolver, final Map<String, From> using) {

        this.resolver = resolver;
        this.using = Immutable.map(using);
    }

    @Override
    public From visitDefault(final Expression expression) {

        throw new UnsupportedOperationException("Expression is not a valid SQL query (" + expression + ")");
    }

    @Override
    public From visitWith(final With expression) {

        final Map<String, From> using = new HashMap<>(this.using);
        FromSqlVisitor visitor = this;
        for (final Pair<String, Expression> entry : expression.getWith()) {
            using.put(entry.getFirst(), visitor.visit(entry.getSecond()));
            visitor = new FromSqlVisitor(resolver, using);
        }
        return visitor.visit(expression.getYield());
    }

    @Override
    public From visitNameConstant(final NameConstant expression) {

        final Name name = expression.getName();
        final From resolved = using.get(name.toString());
        if (resolved != null) {
            return resolved;
        } else {
            final LinkableSchema schema = resolver.requireLinkableSchema(name);
            return new FromSchema(schema, ImmutableSet.of());
        }
    }

    private From buildFrom(final io.basestar.expression.sql.From from) {

        return from.visit(new io.basestar.expression.sql.From.Visitor<From>() {
            @Override
            public From visitAnonymous(final io.basestar.expression.sql.From.Anonymous from) {

                final From result = visit(from.getExpression());
                if (from.getExpression() instanceof NameConstant) {
                    final Name name = ((NameConstant) from.getExpression()).getName();
                    if (name.size() == 1) {
                        return result.as(name.first());
                    }
                }
                return result;
            }

            @Override
            public From visitNamed(final io.basestar.expression.sql.From.Named from) {

                return visit(from.getExpression()).as(from.getName());
            }

            @Override
            public From visitJoin(final io.basestar.expression.sql.From.Join from) {

                final From left = buildFrom(from.getLeft());
                final From right = buildFrom(from.getRight());
                final Join.Type type = Join.Type.valueOf(from.getType().name());
                final Expression on = from.getOn();
                return new FromJoin(new Join(left, right, type, on));
            }
        });
    }

    @Override
    public From visitSql(final Sql expression) {

        final List<Select> selects = expression.getSelect();
        final Expression where = expression.getWhere();
        final List<io.basestar.expression.sql.From> froms = expression.getFrom();
        final List<Expression> group = expression.getGroup();
        final List<Union> unions = expression.getUnion();

        From result;
        if (froms.size() == 1) {
            result = buildFrom(froms.get(0));
        } else {
            result = null;
            for (final io.basestar.expression.sql.From from : froms) {
                if (result == null) {
                    result = buildFrom(from);
                } else {
                    // FIXME
                    result = result.join(buildFrom(from), Join.Type.INNER, new Constant(true));
                }
            }
            // Old-style join
//            throw new UnsupportedOperationException();
        }

        if (where != null) {
            result = result.filter(where);
        }

        if (selects != null) {

            final Map<String, Expression> inputs = new HashMap<>();
            for (final Select select : selects) {
                select.visit(new Select.Visitor<Object>() {
                    @Override
                    public Object visitAll(final Select.All from) {

                        return null;
                    }

                    @Override
                    public Object visitAnonymous(final Select.Anonymous from) {

                        return inputs.put(selectName(from.getExpression(), inputs), from.getExpression());
                    }

                    @Override
                    public Object visitNamed(final Select.Named from) {

                        return inputs.put(from.getName(), from.getExpression());
                    }
                });
            }

            if (!inputs.isEmpty()) {

                if (group != null && !group.isEmpty()) {

                    final List<String> groupNames = new ArrayList<>();
                    for (final Expression g : group) {
                        final String name = selectName(g, inputs);
                        inputs.put(name, g);
                        groupNames.add(name);
                    }
                    result = result.agg(groupNames, inputs);
                } else {
                    result = result.select(inputs);
                }
            }
        }

        if (unions != null && !unions.isEmpty()) {
            final List<From> inputs = new ArrayList<>();
            inputs.add(result);
            for (final Union union : unions) {
                union.visit(new Union.Visitor<Object>() {
                    @Override
                    public Object visitDistinct(final Union.Distinct from) {

                        return inputs.add(visit(from.getExpr()));
                    }

                    @Override
                    public Object visitAll(final Union.All from) {

                        return inputs.add(visit(from.getExpr()));
                    }
                });
            }
            result = new FromUnion(inputs, false);
        }

        return result;
    }

    private String selectName(final Expression expression, final Map<String, Expression> select) {

        if (expression instanceof NameConstant) {
            final Name name = ((NameConstant) expression).getName();
            final String last = name.last();
            final Expression other = select.get(last);
            if (other == null || expression.equals(other)) {
                return last;
            }
        }
        return "v" + expression.digest();
    }
}