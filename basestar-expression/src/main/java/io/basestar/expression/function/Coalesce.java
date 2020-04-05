package io.basestar.expression.function;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.logical.Or;
import lombok.Data;

/**
 * Coalesce
 *
 * Return the first non-null operand
 */

@Data
public class Coalesce implements Binary {

    public static final String TOKEN = "??";

    public static final int PRECEDENCE = Or.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs ?? rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Coalesce(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Coalesce(lhs, rhs);
    }

    @Override
    public Object evaluate(final Context context) {

        final Object lhs = this.lhs.evaluate(context);
        if(lhs == null) {
            return this.rhs.evaluate(context);
        } else {
            return lhs;
        }
    }

//    @Override
//    public Query query() {
//
//        throw new UnsupportedOperationException(TOKEN);
//    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitCoalesce(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }
}
