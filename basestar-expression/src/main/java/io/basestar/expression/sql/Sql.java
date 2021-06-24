package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.function.Lambda;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Sql implements Expression {

    public static final String TOKEN = "SELECT";

    public static final int PRECEDENCE = Lambda.PRECEDENCE + 1;

    private final List<Select> select;

    private final List<From> from;

    private final Expression where;

    private final List<Name> group;

    private final List<Sort> order;

    private final List<Union> union;

    public Sql(final List<Select> select, final List<From> from, final Expression where, final List<Name> group, final List<Sort> order, final List<Union> union) {

        this.select = Immutable.list(select);
        this.from = Immutable.list(from);
        this.where = where;
        this.group = Immutable.list(group);
        this.order = Immutable.list(order);
        this.union = Immutable.list(union);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        return this;
    }

    @Override
    public Object evaluate(final Context context) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Name> names() {

        return null;
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

        return null;
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        return null;
    }
}
