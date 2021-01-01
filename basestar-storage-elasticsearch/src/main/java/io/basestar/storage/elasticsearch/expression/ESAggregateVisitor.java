package io.basestar.storage.elasticsearch.expression;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.aggregate.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ESAggregateVisitor implements ExpressionVisitor.Defaulting<ESAggregate> {

    private final InferenceContext inference;

    @Override
    public ESAggregate visitDefault(final Expression expression) {

        throw new UnsupportedOperationException(expression + " is not (yet) a supported aggregate");
    }

    @Override
    public ESAggregate visitAvg(final Avg expression) {

        final Expression input = expression.getInput();
        return new ESAggregate.Avg(input);
    }

    @Override
    public ESAggregate visitCount(final Count expression) {

        final Expression predicate = expression.getPredicate();
        if (predicate.isConstant()) {
            return new ESAggregate.Count();
        } else {
            return new ESAggregate.CountIf(predicate);
        }
    }

    @Override
    public ESAggregate visitMax(final Max expression) {

        final Expression input = expression.getInput();
        final Use<?> typeOf = inference.typeOf(input);
        if(typeOf.isNumeric()) {
            return new ESAggregate.NumberMax(input);
        } else if(typeOf.isStringLike()) {
            return new ESAggregate.StringMax(input);
        } else {
            throw new UnsupportedOperationException("Max/min only supported on numeric or string-like fields");
        }
    }

    @Override
    public ESAggregate visitMin(final Min expression) {

        final Expression input = expression.getInput();
        final Use<?> typeOf = inference.typeOf(input);
        if(typeOf.isNumeric()) {
            return new ESAggregate.NumberMin(input);
        } else if(typeOf.isStringLike()) {
            return new ESAggregate.StringMin(input);
        } else {
            throw new UnsupportedOperationException("Max/min only supported on numeric or string-like fields");
        }
    }

    @Override
    public ESAggregate visitSum(final Sum expression) {

        final Expression input = expression.getInput();
        return new ESAggregate.Sum(input);
    }
}
