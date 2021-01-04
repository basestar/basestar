package io.basestar.expression.iterate;

import com.google.common.collect.Streams;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Name;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface For extends Expression {

    List<Expression> getYields();

    ContextIterator getIterator();

    @Override
    @SuppressWarnings("UnstableApiUsage")
    default Expression bind(final Context context, final Renaming root) {

        final List<Expression> beforeYield = getYields();
        final ContextIterator beforeIterator = getIterator();
        final List<Expression> afterYield = bindYield(context, root);
        final ContextIterator afterIterator = beforeIterator.bind(context, root);
        final Expression bound;
        if(beforeIterator == afterIterator && Streams.zip(beforeYield.stream(), afterYield.stream(), (a, b) -> a == b).allMatch(v -> v)) {
            bound = this;
        } else {
            bound = create(afterYield, afterIterator);
        }
        if(bound.isConstant()) {
            return new Constant(bound.evaluate(context));
        } else {
            return bound;
        }
    }

    Expression create(List<Expression> yields, ContextIterator iterator);

    default List<Expression> bindYield(final Context context, final Renaming root) {

        return getYields().stream().map(y -> y.bind(context, Renaming.closure(getIterator().closure(), root)))
                .collect(Collectors.toList());
    }

    @Override
    default Set<Name> names() {

        final ContextIterator iterator = getIterator();
        final Set<String> closure = iterator.closure();
        final Set<Name> names = new HashSet<>();
        getYields().forEach(y -> names.addAll(y.names()));
        names.removeIf(v -> closure.contains(v.first()));
        names.addAll(iterator.names());
        return names;
    }

    @Override
    default boolean isConstant(final Closure closure) {

        final ContextIterator iterator = getIterator();
        final List<Expression> yields = getYields();

        return iterator.isConstant(closure) && yields.stream()
                .allMatch(y -> y.isConstant(closure.with(iterator.closure())));
    }

    @Override
    default List<Expression> expressions() {

        final List<Expression> expressions = new ArrayList<>();
        getYields().forEach(y -> expressions.addAll(y.expressions()));
        expressions.addAll(getIterator().expressions());
        return expressions;
    }

    @Override
    default Expression copy(final List<Expression> expressions) {

        List<Expression> remaining = expressions;
        final List<Expression> yields = new ArrayList<>();
        for(final Expression yield : getYields()) {
            final int yieldSize = yield.expressions().size();
            assert remaining.size() >= yieldSize;
            yields.add(yield.copy(remaining.subList(0, yieldSize)));
            remaining = remaining.subList(yieldSize, remaining.size());
        }
        return create(yields, getIterator().copy(remaining));
    }
}
