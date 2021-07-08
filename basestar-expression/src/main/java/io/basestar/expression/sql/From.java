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

    <T> T visit(Visitor<T> visitor);

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

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAnonymous(this);
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

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitNamed(this);
        }
    }

    @Data
    class Join implements From {

        public enum Type {

            INNER,
            LEFT_OUTER,
            RIGHT_OUTER,
            FULL_OUTER
        }

        private final From left;

        private final From right;

        private final Expression on;

        private final Join.Type type;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return left.isConstant(closure) && right.isConstant(closure) && on.isConstant(closure.with(names()));
        }

        @Override
        public Set<String> names() {

            return Stream.concat(left.names().stream(), right.names().stream()).collect(Collectors.toSet());
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitJoin(this);
        }
    }

    interface Visitor<T> {

        T visitAnonymous(Anonymous from);

        T visitNamed(Named from);

        T visitJoin(Join from);
    }
}
