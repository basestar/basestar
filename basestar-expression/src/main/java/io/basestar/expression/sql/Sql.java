package io.basestar.expression.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.function.Lambda;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class Sql implements Expression {

    public static final String TOKEN = "SELECT";

    public static final int PRECEDENCE = Lambda.PRECEDENCE + 1;

    private final List<Select> select;

    private final List<From> from;

    @Nullable
    private final Expression where;

    private final List<Expression> group;

    private final List<Sort> order;

    private final List<Union> union;

    public Sql(final List<Select> select, final List<From> from, @Nullable final Expression where, final List<Expression> group, final List<Sort> order, final List<Union> union) {

        this.select = Immutable.list(select);
        this.from = Immutable.list(from);
        this.where = where;
        this.group = Immutable.list(group);
        this.order = Immutable.list(order);
        this.union = Immutable.list(union);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        return new Sql(
                Immutable.transform(select, v -> v.bind(context, root)),
                Immutable.transform(from, v -> v.bind(context, root)),
                where == null ? null : where.bind(context, root),
                Immutable.transform(group, v -> v.bind(context, root)),
                order,
                Immutable.transform(union, v -> v.bind(context, root))
        );
    }

    @Override
    public Object evaluate(final Context context) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Name> names() {

        return ImmutableSet.of();
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return 0;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitSql(this);
    }

    @Override
    public boolean isConstant(final Closure closure) {

        return false;
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(where);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {

        final StringBuilder str = new StringBuilder();
        str.append("SELECT ");
        str.append(select.stream().map(Select::toString).collect(Collectors.joining(", ")));
        str.append(" FROM ");
        str.append(from.stream().map(From::toString).collect(Collectors.joining(", ")));
        if(where != null) {
            str.append(" WHERE ");
            str.append(where);
        }
        if(!group.isEmpty()) {
            str.append(" GROUP BY ");
            str.append(group.stream().map(Expression::toString).collect(Collectors.joining(", ")));
        }
        if(!order.isEmpty()) {
            str.append(" ORDER BY ");
            str.append(order.stream().map(s -> s.getName() + " " + s.getOrder().name()).collect(Collectors.joining(", ")));
        }
        if(!union.isEmpty()) {
            str.append(union.stream().map(Union::toString).collect(Collectors.joining(" ")));
        }
        return str.toString();
    }
}
