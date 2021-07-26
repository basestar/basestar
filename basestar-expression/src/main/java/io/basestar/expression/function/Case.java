package io.basestar.expression.function;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.sql.Sql;
import io.basestar.expression.type.Values;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.Data;

import java.util.*;

public interface Case extends Expression {

    String TOKEN = "CASE";

    int PRECEDENCE = Sql.PRECEDENCE + 1;

    List<Pair<Expression, Expression>> getWhen();

    Expression getOtherwise();

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

            final Expression with = this.with.bind(context, root);
            boolean changed = with != this.with;
            final List<Pair<Expression, Expression>> when = new ArrayList<>();
            for(final Pair<Expression, Expression> entry : this.when) {
                final Expression first = entry.getFirst().bind(context, root);
                final Expression second = entry.getSecond().bind(context, root);
                if(first != entry.getFirst() || second != entry.getSecond()) {
                    changed = true;
                }
                when.add(Pair.of(first, second));
            }
            final Expression otherwise;
            if(this.otherwise != null) {
                otherwise = this.otherwise.bind(context, root);
                changed = changed || otherwise != this.otherwise;
            } else {
                otherwise = null;
            }
            if(changed) {
                return new Simple(with, when, otherwise);
            } else {
                return this;
            }
        }

        @Override
        public Object evaluate(final Context context) {

            final Object value = with.evaluate(context);
            for(final Pair<Expression, Expression> entry : when) {
                final Object test = entry.getFirst().evaluate(context);
                if(Values.equals(value, test)) {
                    return entry.getSecond().evaluate(context);
                }
            }
            if(otherwise != null) {
                return otherwise.evaluate(context);
            } else {
                return null;
            }
        }

        @Override
        public Set<Name> names() {

            final Set<Name> names = new HashSet<>(with.names());
            for(final Pair<Expression, Expression> entry : when) {
                names.addAll(entry.getFirst().names());
                names.addAll(entry.getSecond().names());
            }
            if(otherwise != null) {
                names.addAll(otherwise.names());
            }
            return names;
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

            boolean changed = false;
            final List<Pair<Expression, Expression>> when = new ArrayList<>();
            for(final Pair<Expression, Expression> entry : this.when) {
                final Expression first = entry.getFirst().bind(context, root);
                final Expression second = entry.getSecond().bind(context, root);
                if(first != entry.getFirst() || second != entry.getSecond()) {
                    changed = true;
                }
                when.add(Pair.of(first, second));
            }
            final Expression otherwise;
            if(this.otherwise != null) {
                otherwise = this.otherwise.bind(context, root);
                changed = changed || otherwise != this.otherwise;
            } else {
                otherwise = null;
            }
            if(changed) {
                return new Searched(when, otherwise);
            } else {
                return this;
            }
        }

        @Override
        public Object evaluate(final Context context) {

            for(final Pair<Expression, Expression> entry : when) {
                if(entry.getFirst().evaluatePredicate(context)) {
                    return entry.getSecond().evaluate(context);
                }
            }
            if(otherwise != null) {
                return otherwise.evaluate(context);
            } else {
                return null;
            }
        }

        @Override
        public Set<Name> names() {

            final Set<Name> names = new HashSet<>();
            for(final Pair<Expression, Expression> entry : when) {
                names.addAll(entry.getFirst().names());
                names.addAll(entry.getSecond().names());
            }
            if(otherwise != null) {
                names.addAll(otherwise.names());
            }
            return names;
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
