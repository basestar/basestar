package io.basestar.expression.literal;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.function.With;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Literal Array
 *
 * Create an array by providing values
 */

@Data
public class LiteralArray implements Expression {

    public static final String TOKEN = "[]";

    public static final int PRECEDENCE = With.PRECEDENCE + 1;

    private final List<Expression> args;

    /**
     * [args...]
     *
     * @param args expression Array values
     */

    public LiteralArray(final List<Expression> args) {

        this.args = args;
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        boolean changed = false;
        boolean constant = true;
        final List<Expression> args = new ArrayList<>();
        for(final Expression before : this.args) {
            final Expression after = before.bind(context, root);
            args.add(after);
            constant = constant && after instanceof Constant;
            changed = changed || before != after;
        }
        if(constant) {
            return new Constant(evaluate(args, context));
        } else if(changed) {
            return new LiteralArray(args);
        } else {
            return this;
        }
    }

    @Override
    public List<?> evaluate(final Context context) {

        return evaluate(args, context);
    }

    private static List<?> evaluate(final List<Expression> args, final Context context) {

        return args.stream()
                .map(v -> v.evaluate(context)).collect(Collectors.toList());
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public Set<Path> paths() {

        return args.stream().flatMap(v -> v.paths().stream())
                .collect(Collectors.toSet());
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

        return visitor.visitLiteralArray(this);
    }

    @Override
    public String toString() {

        return Values.toString(args);
    }
}
