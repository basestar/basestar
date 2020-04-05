package io.basestar.expression.compare;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.type.Values;
import lombok.Data;

/**
 * Less Than
 */

@Data
public class Lt implements Binary {

    public static final String TOKEN = "<";

    public static final int PRECEDENCE = Gte.PRECEDENCE;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs < rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Lt(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Lt(lhs, rhs);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return Values.compare(lhs.evaluate(context), rhs.evaluate(context)) < 0;
    }

//    @Override
//    public Query query() {
//
//        if(lhs instanceof PathConstant && rhs instanceof Constant) {
//            return Query.lt(((PathConstant) lhs).getPath(), ((Constant) rhs).getValue());
//        } else if(lhs instanceof Constant && rhs instanceof PathConstant) {
//            return Query.gte(((PathConstant) rhs).getPath(), ((Constant) lhs).getValue());
//        } else {
//            return Query.and();
//        }
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

        return visitor.visitLt(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }
}
