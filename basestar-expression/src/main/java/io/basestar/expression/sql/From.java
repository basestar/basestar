package io.basestar.expression.sql;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import lombok.Data;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface From {

    boolean isConstant(Expression.Closure closure);

    Set<String> names();

    @Data
    class Anonymous implements From {

        private final Expression expression;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public Set<String> names() {

            return ImmutableSet.of();
        }
    }

    @Data
    class Named implements From {

        private final Expression expression;

        private final String name;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public Set<String> names() {

            return ImmutableSet.of(name);
        }
    }

    @Data
    class Join implements From {

        public enum Side {

            LEFT,
            RIGHT
        }

        public enum Type {

            INNER,
            OUTER
        }

        private final From left;

        private final From right;

        private final Expression on;

        private final Join.Side side;

        private final Join.Type type;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return left.isConstant(closure) && right.isConstant(closure) && on.isConstant(closure.with(names()));
        }

        @Override
        public Set<String> names() {

            return Stream.concat(left.names().stream(), right.names().stream()).collect(Collectors.toSet());
        }
    }
}
