package io.basestar.expression.function;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import lombok.Data;

@Data
public class Operator implements Binary {

    public static final int PRECEDENCE = Coalesce.PRECEDENCE + 1;

    private final String name;

    private final Expression lhs;

    private final Expression rhs;

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Operator(name, lhs, rhs);
    }

    @Override
    public Object evaluate(final Context context) {

        // FIXME:
        throw new UnsupportedOperationException();
    }

    @Override
    public String token() {

        return name;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitOperator(this);
    }

    @Override
    public String toString() {

        return lhs + " " +  name + " " + rhs;
    }
}
