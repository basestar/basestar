package io.basestar.expression.iterate;

import com.google.common.collect.Streams;
import io.basestar.expression.*;
import lombok.Data;

import java.util.Iterator;
import java.util.Map;

/**
 * Where
 *
 * Filter an iterator using a predicate
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class Where implements Binary {

    public static final String TOKEN = "where";

    public static final int PRECEDENCE = Of.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs where rhs
     *
     * @param lhs iterator Iterator
     * @param rhs predicate Expression to be evaluated for each iterator
     */

    public Where(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Where(lhs, rhs);
    }

    @Override
    public Expression bindRhs(final Context context, final PathTransform root) {

        return getRhs().bind(context, PathTransform.closure(getLhs().closure(), root));
    }

    @Override
    public Iterator<?> evaluate(final Context context) {

        final Object lhs = this.lhs.evaluate(context);
        if(lhs instanceof Iterator<?>) {
            return Streams.stream((Iterator<?>)lhs)
                    .filter(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> values = (Map<String, Object>)v;
                        return rhs.evaluatePredicate(context.with(values));
                    })
                    .iterator();
        } else {
            throw new IllegalStateException();
        }
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

        return visitor.visitWhere(this);
    }

    @Override
    public String toString() {

        return lhs + " " + TOKEN + " " + rhs;
    }
}
