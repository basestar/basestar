package io.basestar.expression.iterate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.util.Path;
import lombok.Data;

import java.util.*;

/**
 * Array Comprehension
 *
 * Create an array from an iterator
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class ForArray implements Expression {

    public static final String TOKEN = "for";

    public static final int PRECEDENCE = ForSet.PRECEDENCE + 1;

    private final Expression yield;

    private final Expression iter;

    /**
     * [yield for iter]
     *
     * @param yield expression Value-yielding expression
     * @param iter iterator Iterator
     */

    public ForArray(final Expression yield, final Expression iter) {

        this.yield = yield;
        this.iter = iter;
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        final Expression yield = this.yield.bind(context, PathTransform.closure(iter.closure(), root));
        final Expression iter = this.iter.bind(context, root);
        if(yield == this.yield && iter == this.iter) {
            return this;
        } else {
            return new ForArray(yield, iter);
        }
    }

    @Override
    public List<?> evaluate(final Context context) {

        final Object iter = this.iter.evaluate(context);
        if(iter instanceof Iterator<?>) {
            final List<Object> result = new ArrayList<>();
            Streams.stream((Iterator<?>)iter)
                    .forEach(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scope = (Map<String, Object>)v;
                        final Object value = this.yield.evaluate(context.with(scope));
                        result.add(value);
                    });
            return result;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<Path> paths() {

        return ImmutableSet.<Path>builder()
                .addAll(yield.paths())
                .addAll(iter.paths())
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

        return visitor.visitForArray(this);
    }

    @Override
    public String toString() {

        return "[" + yield + " " + TOKEN + " " + iter + "]";
    }
}
