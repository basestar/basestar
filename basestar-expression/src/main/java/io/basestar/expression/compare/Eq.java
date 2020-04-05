package io.basestar.expression.compare;

import io.basestar.expression.*;
import io.basestar.expression.type.Values;
import lombok.Data;

import java.util.function.BiFunction;

/**
 * Equals
 */

@Data
public class Eq implements Binary {

    public static final String TOKEN = "==";

    public static final int PRECEDENCE = Gte.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs == rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Eq(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Eq(lhs, rhs);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return Values.equals(lhs.evaluate(context), rhs.evaluate(context));
    }

//    @Override
//    public Query query() {
//
//        if(lhs instanceof PathConstant && rhs instanceof Constant) {
//            return Query.eq(((PathConstant) lhs).getPath(), ((Constant) rhs).getValue());
//        } else if(lhs instanceof Constant && rhs instanceof PathConstant) {
//            return Query.eq(((PathConstant) rhs).getPath(), ((Constant) lhs).getValue());
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

        return visitor.visitEq(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }

//    public static Matcher<Eq> matcher() {
//
//        return matcher(lhs -> lhs, rhs -> rhs, (a, b) -> this);
//    }
//
//    public static <A, B> Matcher<Eq> matcher(final Matcher<A> lhs, final Matcher<B> rhs) {
//
//        return matcher(lhs, rhs, (a, b) -> this);
//    }

    public static <A, B, R> Matcher<R> match(final Matcher<A> lhs, final Matcher<B> rhs, final BiFunction<A, B, R> then) {

        return root -> {
            if(root instanceof Eq) {
                final Eq eq = (Eq)root;
                final A matchA = lhs.match(eq.getLhs());
                final B matchB = rhs.match(eq.getRhs());
                if (matchA != null && matchB != null) {
                    return then.apply(matchA, matchB);
                }
            }
            return null;
        };
    }
}
