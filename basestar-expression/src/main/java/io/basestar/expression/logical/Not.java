package io.basestar.expression.logical;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Unary;
import io.basestar.expression.arithmetic.Negate;
import lombok.Data;

/**
 * Not
 *
 * Logical Complement
 */

@Data
public class Not implements Unary {

    public static final String TOKEN = "!";

    public static final int PRECEDENCE = Negate.PRECEDENCE;

    private final Expression operand;

    /**
     * !operand
     *
     * @param operand any Operand
     */

    public Not(final Expression operand) {

        this.operand = operand;
    }

    @Override
    public Expression create(final Expression value) {

        return new Not(value);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return !operand.evaluatePredicate(context);
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

        return visitor.visitNot(this);
    }

    @Override
    public String toString() {

        return Unary.super.toString(operand);
    }
}
