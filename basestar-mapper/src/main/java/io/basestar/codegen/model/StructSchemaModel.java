package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.StructSchema;

public class StructSchemaModel extends InstanceSchemaModel {

    private final StructSchema schema;

    public StructSchemaModel(final CodegenSettings settings, final StructSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.StructSchema.class;
    }

    @Override
    public StructSchemaModel getExtend() {

        final StructSchema extend = schema.getExtend();
        if(extend != null) {
            return new StructSchemaModel(getSettings(), extend);
        } else {
            return null;
        }
    }
}
