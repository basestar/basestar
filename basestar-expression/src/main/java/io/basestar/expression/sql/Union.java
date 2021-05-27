package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.util.Name;

import java.util.List;
import java.util.Set;

public class Union implements Expression {

    private final Expression left;

    private final Expression right;

    public Union(final Expression left, final Expression right) {

        this.left = left;
        this.right = right;
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

        return visitor.visitUnion(this);
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
