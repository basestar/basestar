package io.basestar.codegen;

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
}
