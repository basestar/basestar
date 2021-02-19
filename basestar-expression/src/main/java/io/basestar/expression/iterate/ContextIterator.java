package io.basestar.expression.iterate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

public interface ContextIterator extends Serializable {

    String TOKEN = "of";

    ContextIterator bind(Context context, Renaming root);

    Stream<Context> evaluate(Context context);

    Set<String> closure();

    Set<Name> names();

    boolean isConstant(Expression.Closure closure);

    List<Expression> expressions();

    ContextIterator copy(List<Expression> expressions);

    @Data
    class Where implements ContextIterator {

        public static final String TOKEN = "where";

        private final ContextIterator iterator;

        private final Expression expr;

        @Override
        public ContextIterator bind(final Context context, final Renaming root) {

            final ContextIterator iterator = this.iterator.bind(context, root);
            final Expression expr = this.expr.bind(context, root);
            if(iterator == this.iterator && expr == this.expr) {
                return this;
            } else {
                return new Where(iterator, expr);
            }
        }

        @Override
        public Stream<Context> evaluate(final Context context) {

            return iterator.evaluate(context).filter(expr::evaluatePredicate);
        }

        @Override
        public Set<String> closure() {

            return Immutable.set();
        }

        @Override
        public Set<Name> names() {

            final Set<String> closure = iterator.closure();
            final Set<Name> names = new HashSet<>(expr.names());
            names.removeIf(v -> closure.contains(v.first()));
            names.addAll(iterator.names());
            return names;
        }

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return iterator.isConstant(closure) && expr.isConstant(closure.with(iterator.closure()));
        }

        @Override
        public List<Expression> expressions() {

            return Immutable.add(iterator.expressions(), expr);
        }

        @Override
        public ContextIterator copy(final List<Expression> expressions) {

            assert expressions.size() >= 1;
            final Expression last = expressions.get(expressions.size() - 1);
            final List<Expression> rest = expressions.subList(0, expressions.size() - 1);
            return new Where(iterator.copy(rest), last);
        }

        @Override
        public String toString() {

            return iterator + " " + TOKEN + " " + expr;
        }
    }

    @Data
    class OfValue implements ContextIterator {

        private final String value;

        private final Expression expr;

        @Override
        public ContextIterator bind(final Context context, final Renaming root) {

            final Expression expr = this.expr.bind(context, root);
            if(expr == this.expr) {
                return this;
            } else {
                return new OfValue(value, expr);
            }
        }

        @Override
        public Stream<Context> evaluate(final Context context) {

            final Object with = this.expr.evaluate(context);
            return evaluate(context, with);
        }

        private Stream<Context> evaluate(final Context context, final Object with) {

            if(with instanceof Collection<?>) {
                return ((Collection<?>) with).stream()
                        .map(v -> context.with(value, v));
            } else if(with instanceof Map<?, ?>) {
                return ((Map<?, ?>)with).values().stream()
                        .map(v -> context.with(value, v));
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public Set<String> closure() {

            return ImmutableSet.of(value);
        }

        @Override
        public Set<Name> names() {

            return expr.names();
        }

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expr.isConstant(closure);
        }

        @Override
        public List<Expression> expressions() {

            return ImmutableList.of(expr);
        }

        @Override
        public ContextIterator copy(final List<Expression> expressions) {

            assert expressions.size() == 1;
            return new OfValue(value, expressions.get(0));
        }

        @Override
        public String toString() {

            return value + " " + TOKEN + " " + expr;
        }
    }

    @Data
    class OfKeyValue implements ContextIterator {

        private final String key;

        private final String value;

        private final Expression expr;

        @Override
        public ContextIterator bind(final Context context, final Renaming root) {

            final Expression expr = this.expr.bind(context, root);
            if(expr == this.expr) {
                return this;
            } else {
                return new OfKeyValue(key, value, expr);
            }
        }

        @Override
        public Stream<Context> evaluate(final Context context) {

            final Object with = this.expr.evaluate(context);
            return evaluate(context, with);
        }

        @SuppressWarnings("UnstableApiUsage")
        private Stream<Context> evaluate(final Context context, final Object with) {

            if(with instanceof Collection<?>) {
                return Streams.mapWithIndex(
                        ((Collection<?>) with).stream(),
                        (v, k) -> {
                            final Map<String, Object> overlay = new HashMap<>();
                            overlay.put(key, k);
                            overlay.put(value, v);
                            return context.with(overlay);
                        });
            } else if(with instanceof Map<?, ?>) {
                return ((Map<?, ?>)with).entrySet().stream()
                        .map(v -> {
                            final Map<String, Object> overlay = new HashMap<>();
                            overlay.put(key, v.getKey());
                            overlay.put(value, v.getValue());
                            return context.with(overlay);
                        });
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public Set<String> closure() {

            return ImmutableSet.of(key, value);
        }

        @Override
        public Set<Name> names() {

            return expr.names();
        }

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expr.isConstant(closure);
        }

        @Override
        public List<Expression> expressions() {

            return ImmutableList.of(expr);
        }

        @Override
        public ContextIterator copy(final List<Expression> expressions) {

            assert expressions.size() == 1;
            return new OfKeyValue(key, value, expressions.get(0));
        }

        @Override
        public String toString() {

            return "(" + key + ", " + value + ")" + " " + TOKEN + " " + expr;
        }
    }
}
