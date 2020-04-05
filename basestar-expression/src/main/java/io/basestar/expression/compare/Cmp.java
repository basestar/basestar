package io.basestar.expression.compare;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.bitwise.BitLsh;
import io.basestar.expression.type.Values;
import lombok.Data;

/**
 * Compare
 */

@Data
public class Cmp implements Binary {

    public static final String TOKEN = "<=>";

    public static final int PRECEDENCE = BitLsh.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs <=> rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Cmp(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Cmp(lhs, rhs);
    }

    @Override
    public Integer evaluate(final Context context) {

        return Values.compare(lhs.evaluate(context), rhs.evaluate(context));
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
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

        return visitor.visitCmp(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }
}
