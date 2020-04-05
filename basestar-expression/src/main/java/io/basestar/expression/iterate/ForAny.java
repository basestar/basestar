package io.basestar.expression.iterate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.basestar.expression.*;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;
import lombok.Data;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * For Any
 *
 * Existential Quantification: Returns true if the provided predicate is true for any iterator result
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class ForAny implements Binary {

    public static final String TOKEN = "for any";

    public static final int PRECEDENCE = ForArray.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs for any rhs
     *
     * @param lhs expression Predicate
     * @param rhs iterator Iterator
     */

    public ForAny(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new ForAny(lhs, rhs);
    }

//    @Override
//    public Expression bind(final Context context, final Path root) {
//
//        final Expression yield = this.yield.bind(context, root);
//        final Expression with = this.with.bind(context, root);
//        if(yield == this.yield && with == this.with) {
//            return this;
//        } else {
//            return new ForAny(yield, with);
//        }
//    }

    @Override
    public Boolean evaluate(final Context context) {

        final Object with = this.rhs.evaluate(context);
        if(with instanceof Iterator<?>) {
            return Streams.stream((Iterator<?>)with)
                    .anyMatch(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scope = (Map<String, Object>)v;
                        final Object value = this.lhs.evaluate(context.with(scope));
                        return Values.isTruthy(value);
                    });
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Expression bindLhs(final Context context, final PathTransform root) {

        return getLhs().bind(context, PathTransform.closure(getRhs().closure(), root));
    }

    @Override
    public Set<Path> paths() {

        return ImmutableSet.<Path>builder()
                .addAll(lhs.paths())
                .addAll(rhs.paths())
                .build();
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

        return visitor.visitForAny(this);
    }

    @Override
    public String toString() {

        return lhs + " " + TOKEN + " " + rhs;
    }
}
