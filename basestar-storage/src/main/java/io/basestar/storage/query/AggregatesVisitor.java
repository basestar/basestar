package io.basestar.storage.query;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.constant.PathConstant;
import io.basestar.storage.aggregate.Aggregate;
import io.basestar.util.Path;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class AggregatesVisitor implements ExpressionVisitor.Defaulting<Expression> {

    private final Map<String, Aggregate> aggregates = new HashMap<>();

    @Override
    public Expression visitDefault(final Expression expression) {

        return expression.copy(this::visit);
    }

    @Override
    public Expression visitLambdaCall(final LambdaCall expression) {

        final Expression with = expression.getWith();
        if(with instanceof PathConstant) {
            final Path path = ((PathConstant) with).getPath();
            if(path.size() == 1) {
                final String name = path.first();
                final Aggregate.Factory factory = Aggregate.factory(name);
                if(factory != null) {
                    final List<Expression> args = expression.getArgs();
                    final Aggregate aggregate = factory.create(args);
                    final String id = "v" + System.identityHashCode(aggregate);
                    aggregates.put(id, aggregate);
                    return new PathConstant(Path.of(id));
                }
            }
        }
        return visitDefault(expression);
    }
}
