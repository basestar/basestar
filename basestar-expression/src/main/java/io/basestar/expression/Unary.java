package io.basestar.expression;

import io.basestar.expression.constant.Constant;
import io.basestar.util.Path;

import java.util.Set;

public interface Unary extends Expression {

    Expression getOperand();

    Expression create(Expression operand);

    @Override
    default Set<Path> paths() {

        return getOperand().paths();
    }

    @Override
    default Expression bind(final Context context, final PathTransform root) {

        final Expression before = getOperand();
        final Expression after = before.bind(context, root);
        if(after instanceof Constant) {
            return new Constant(create(after).evaluate(context));
        } else if(before == after) {
            return this;
        } else {
            return create(after);
        }
    }

    default String toString(final Expression value) {

        final StringBuilder str = new StringBuilder();
        str.append(token());
        if(value.precedence() > precedence()) {
            str.append("(");
            str.append(value);
            str.append(")");
        } else {
            str.append(value);
        }
        return str.toString();
    }
}
