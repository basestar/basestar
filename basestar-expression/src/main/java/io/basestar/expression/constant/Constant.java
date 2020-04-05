package io.basestar.expression.constant;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;
import lombok.Data;

import java.util.Collections;
import java.util.Set;

@Data
public class Constant implements Expression {

    private static final String TOKEN = "";

    public static final int PRECEDENCE = 0;

    private final Object value;

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        return this;
    }

    @Override
    public Object evaluate(final Context context) {

        return value;
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public Set<Path> paths() {

        return Collections.emptySet();
    }

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

        return visitor.visitConstant(this);
    }

    @Override
    public String toString() {

        return Values.toString(value);
    }
}
