package io.basestar.expression.compare;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.type.Values;
import lombok.Data;

/**
 * Not Equal
 */

@Data
public class Ne implements Binary {

    public static final String TOKEN = "!=";

    public static final int PRECEDENCE = Eq.PRECEDENCE;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs != rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Ne(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Ne(lhs, rhs);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return !Values.equals(lhs.evaluate(context), rhs.evaluate(context));
    }

//    @Override
//    public Query query() {
//
//        if(lhs instanceof PathConstant && rhs instanceof Constant) {
//            return Query.ne(((PathConstant) lhs).getPath(), ((Constant) rhs).getValue());
//        } else if(lhs instanceof Constant && rhs instanceof PathConstant) {
//            return Query.ne(((PathConstant) rhs).getPath(), ((Constant) lhs).getValue());
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

        return visitor.visitNe(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }
}
