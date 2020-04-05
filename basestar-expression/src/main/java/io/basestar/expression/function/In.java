package io.basestar.expression.function;

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.bitwise.BitOr;
import io.basestar.expression.type.Values;
import lombok.Data;

import java.util.Collection;

/**
 * In
 *
 * Set membership
 *
 */

@Data
public class In implements Binary {

    public static final String TOKEN = "in";

    public static final int PRECEDENCE = BitOr.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs in rhs
     *
     * @param lhs any Left hand operand
     * @param rhs collection Right hand operand
     */

    public In(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new In(lhs, rhs);
    }

    @Override
    public Object evaluate(final Context context) {

        final Object lhs = this.lhs.evaluate(context);
        final Object rhs = this.rhs.evaluate(context);
        if(rhs instanceof Collection<?>) {
            return ((Collection<?>) rhs).stream().anyMatch(r -> Values.equals(lhs, r));
        } else {
            throw new IllegalStateException();
        }
    }

//    @Override
//    public Query query() {
//
//        if(lhs instanceof PathConstant && rhs instanceof Constant) {
//            final Path path= ((PathConstant)lhs).getPath();
//            final Object value = ((Constant)rhs).getValue();
//            if(value instanceof Collection<?>) {
//                return Query.or(((Collection<?>) value).stream()
//                        .map(v -> Query.eq(path, v))
//                        .collect(Collectors.toList()));
//            } else {
//                throw new IllegalStateException();
//            }
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

        return visitor.visitIn(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }
}
