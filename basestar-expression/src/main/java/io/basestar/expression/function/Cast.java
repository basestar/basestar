package io.basestar.expression.function;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Unary;
import io.basestar.expression.call.LambdaCall;
import lombok.Data;

@Data
public class Cast implements Unary {

    public static final String TOKEN = "CAST()";

    public static final int PRECEDENCE = LambdaCall.PRECEDENCE;

    private final Expression operand;

    private final String type;

    /**
     * CAST(operand as type)
     *
     * @param operand any Operand
     * @param type name Target type
     */

    public Cast(final Expression operand, final String type) {

        this.operand = operand;
        this.type = type;
    }

    @Override
    public Object evaluate(final Context context) {

        return context.cast(operand.evaluate(context), type);
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

        return visitor.visitCast(this);
    }

    @Override
    public String toString() {

        return "CAST(" + operand + " AS " + type + ")";
    }

    @Override
    public Expression create(final Expression operand) {

        return new Cast(operand, type);
    }
}
