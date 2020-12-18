package io.basestar.spark.query;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.Reserved;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
@Getter
@lombok.Builder(builderClassName = "Builder")
public class FilterStepTransform implements FoldingStepTransform {

    private final Layout layout;

    private final Expression expression;

    @Override
    public Layout getInputLayout() {

        return layout;
    }

    @Override
    public Layout getOutputLayout() {

        return layout;
    }

    @Override
    public Arity getArity() {

        return Arity.FILTERING;
    }

    @Override
    public Step step() {

        final Expression expression = this.expression;
        return input -> {

            try {
                final Context context = Context.init(input).with(Reserved.THIS, input);
                if(expression.evaluatePredicate(context)) {
                    return Collections.singleton(input).iterator();
                }
            } catch (final Exception e) {
                log.error("Failed to process filter expression " + expression + " (" + e.getMessage() + ")");
            }
            return Collections.emptyIterator();
        };
    }
}
