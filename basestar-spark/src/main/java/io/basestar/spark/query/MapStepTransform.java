package io.basestar.spark.query;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.Reserved;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
@lombok.Builder(builderClassName = "Builder")
public class MapStepTransform implements FoldingStepTransform {

    private final Layout inputLayout;

    private final Layout outputLayout;

    private final Map<String, Expression> expressions;

    @Override
    public Arity getArity() {

        return Arity.MAPPING;
    }

    @Override
    public Step step() {

        final Map<String, Expression> expressions = this.expressions;
        return input -> {

            final Context context = Context.init(input).with(Reserved.THIS, input);
            final Map<String, Object> output = new HashMap<>();
            for(final Map.Entry<String, Expression> entry : expressions.entrySet()) {
                try {
                    output.put(entry.getKey(), entry.getValue().evaluate(context));
                } catch (final Exception e) {
                    log.error("Failed to process map expression " + entry.getValue() + " (" + e.getMessage() + ")");
                }
            }
            return Collections.singleton(output).iterator();
        };
    }
}
