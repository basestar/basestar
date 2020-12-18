package io.basestar.spark.query;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Layout;
import lombok.Getter;

import java.util.Collections;

@Getter
@lombok.Builder(builderClassName = "Builder")
public class SchemaStepTransform implements FoldingStepTransform {

    private final Layout inputLayout;

    private final InstanceSchema schema;

    @Override
    public Layout getOutputLayout() {

        return schema;
    }

    @Override
    public Arity getArity() {

        return Arity.MAPPING;
    }

    @Override
    public Step step() {

        final InstanceSchema schema = this.schema;
        return input -> Collections.singleton(schema.create(input, schema.getExpand(), true)).iterator();
    }
}
