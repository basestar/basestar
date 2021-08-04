package io.basestar.expression.function;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.call.LambdaCall;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class BinaryConcat implements Expression {

    private static final String TOKEN = "BCAT";

    private static final int PRECEDENCE = LambdaCall.PRECEDENCE;

    final List<Expression> input;

    public BinaryConcat(final List<Expression> input) {

        this.input = Immutable.list(input);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final List<Expression> input = new ArrayList<>();
        boolean changed = false;
        for (final Expression before : this.input) {
            final Expression after = before.bind(context, root);
            changed = changed || (before != after);
            input.add(after);
        }
        return changed ? new BinaryConcat(input) : this;
    }

    @Override
    public Object evaluate(final Context context) {

        final List<Object> values = input.stream().map(v -> v.evaluate(context)).collect(Collectors.toList());
        return BinaryKey.from(values);
    }

    @Override
    public Set<Name> names() {

        return input.stream().flatMap(e -> e.names().stream()).collect(Collectors.toSet());
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

        return visitor.visitBinaryConcat(this);
    }

    @Override
    public boolean isConstant(final Closure closure) {

        return input.stream().allMatch(e -> e.isConstant(closure));
    }

    @Override
    public List<Expression> expressions() {

        return input;
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        return new BinaryConcat(expressions);
    }
}
