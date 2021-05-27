package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Select implements Expression {

    private final List<Expression> select;

    private final Expression from;

    private final Expression where;

    private final List<Name> group;

    private final List<Name> order;

    public Select(final List<Expression> select, final Expression from, final Expression where, final List<Name> group, final List<Name> order) {

        this.select = Immutable.list(select);
        this.from = from;
        this.where = where;
        this.group = Immutable.list(group);
        this.order = Immutable.list(order);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {
        return null;
    }

    @Override
    public Object evaluate(final Context context) {
        return null;
    }

    @Override
    public Set<Name> names() {
        return null;
    }

    @Override
    public String token() {
        return null;
    }

    @Override
    public int precedence() {
        return 0;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitSelect(this);
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
