package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Join implements Expression {

    public enum Side {

        LEFT,
        RIGHT
    }

    public enum Type {

        INNER,
        OUTER
    }

    private final Expression left;

    private final Expression right;

    private final Side side;

    private final Type type;

    public Join(final Expression left, final Expression right, final Side side, final Type type) {

        this.left = left;
        this.right = right;
        this.side = Nullsafe.orDefault(side, Side.LEFT);
        this.type = Nullsafe.orDefault(type, Type.INNER);
    }

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

        return visitor.visitJoin(this);
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
