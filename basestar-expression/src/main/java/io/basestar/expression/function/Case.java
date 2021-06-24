package io.basestar.expression.function;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.sql.Sql;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Case implements Expression {

    private static final String TOKEN = "case";

    public static final int PRECEDENCE = Sql.PRECEDENCE + 1;

    private final List<Pair<Expression, Expression>> when;

    private final Expression otherwise;

    @Override
    public Expression bind(final Context context, final Renaming root) {

        return null;
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
    public String token() {

        return null;
    }

    @Override
    public int precedence() {

        return 0;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return null;
    }

    @Override
    public boolean isConstant(final Closure closure) {

        return false;
    }

    @Override
    public List<Expression> expressions() {

        return null;
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        return null;
    }
}
