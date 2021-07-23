package io.basestar.expression.function;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.sql.Sql;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Case extends Expression {

    String TOKEN = "CASE";

    int PRECEDENCE = Sql.PRECEDENCE + 1;

    @Override
    default String token() {

        return TOKEN;
    }

    @Override
    default int precedence() {

        return PRECEDENCE;
    }

    @Override
    default <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitCase(this);
    }

    @Data
    class Simple implements Case {

        private final Expression with;

        private final List<Pair<Expression, Expression>> when;

        private final Expression otherwise;

        public Simple(final Expression with, final List<Pair<Expression, Expression>> when, final Expression otherwise) {

            this.with = with;
            this.when = Immutable.list(when);
            this.otherwise = otherwise;
        }

        @Override
        public Expression bind(final Context context, final Renaming root) {

            return this;
        }

        @Override
        public Object evaluate(final Context context) {

            return null;
        }

        @Override
        public Set<Name> names() {

            return null;
        }

        @Override
        public boolean isConstant(final Closure closure) {

            return with.isConstant(closure)
                    && when.stream().allMatch(pair -> pair.getFirst().isConstant(closure) && pair.getSecond().isConstant(closure))
                    && Optional.ofNullable(otherwise).map(v -> v.isConstant(closure)).orElse(true);
        }

        @Override
        public List<Expression> expressions() {

            final List<Expression> result = new ArrayList<>();
            result.add(with);
            when.forEach(pair -> {
                result.add(pair.getFirst());
                result.add(pair.getSecond());
            });
            if(otherwise != null) {
                result.add(otherwise);
            }
            return result;
        }

        @Override
        public Expression copy(final List<Expression> expressions) {

            final Expression with = expressions.get(0);
            final List<Pair<Expression, Expression>> when = new ArrayList<>();
            for(int i = 0; i != this.when.size(); ++i) {
                when.add(Pair.of(expressions.get(i * 2 + 1), expressions.get(i * 2 + 2)));
            }
            final Expression otherwise = this.otherwise == null ? null : expressions.get(expressions.size() - 1);
            return new Simple(with, when, otherwise);
        }

        @Override
        public String toString() {

            final StringBuilder str = new StringBuilder();
            str.append(TOKEN);
            str.append(" ");
            str.append(with);
            for(final Pair<Expression, Expression> entry : when) {
                str.append(" WHEN ");
                str.append(entry.getFirst());
                str.append(" THEN ");
                str.append(entry.getSecond());
            }
            if(otherwise != null) {
                str.append(" ELSE ");
                str.append(otherwise);
            }
            str.append(" END");
            return str.toString();
        }
    }

    @Data
    class Searched implements Case {

        private final List<Pair<Expression, Expression>> when;

        private final Expression otherwise;

        public Searched(final List<Pair<Expression, Expression>> when, final Expression otherwise) {

            this.when = Immutable.list(when);
            this.otherwise = otherwise;
        }

        @Override
        public Expression bind(final Context context, final Renaming root) {

            return this;
        }

        @Override
        public Object evaluate(final Context context) {

            return null;
        }


        @Override
        public Set<Name> names() {

            return null;
        }

        @Override
        public boolean isConstant(final Closure closure) {

            return when.stream().allMatch(pair -> pair.getFirst().isConstant(closure) && pair.getSecond().isConstant(closure))
                    && Optional.ofNullable(otherwise).map(v -> v.isConstant(closure)).orElse(true);
        }

        @Override
        public List<Expression> expressions() {

            final List<Expression> result = new ArrayList<>();
            when.forEach(pair -> {
                result.add(pair.getFirst());
                result.add(pair.getSecond());
            });
            if(otherwise != null) {
                result.add(otherwise);
            }
            return result;
        }

        @Override
        public Expression copy(final List<Expression> expressions) {

            final List<Pair<Expression, Expression>> when = new ArrayList<>();
            for(int i = 0; i != this.when.size(); ++i) {
                when.add(Pair.of(expressions.get(i * 2), expressions.get(i * 2 + 1)));
            }
            final Expression otherwise = this.otherwise == null ? null : expressions.get(expressions.size() - 1);
            return new Searched(when, otherwise);
        }

        @Override
        public String toString() {

            final StringBuilder str = new StringBuilder();
            str.append(TOKEN);
            for(final Pair<Expression, Expression> entry : when) {
                str.append(" WHEN ");
                str.append(entry.getFirst());
                str.append(" THEN ");
                str.append(entry.getSecond());
            }
            if(otherwise != null) {
                str.append(" ELSE ");
                str.append(otherwise);
            }
            str.append(" END");
            return str.toString();
        }
    }
}
