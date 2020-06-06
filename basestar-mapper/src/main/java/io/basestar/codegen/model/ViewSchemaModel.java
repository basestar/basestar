package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.ViewSchema;

public class ViewSchemaModel extends InstanceSchemaModel {

    private final ViewSchema schema;

    public ViewSchemaModel(final CodegenSettings settings, final ViewSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.ViewSchema.class;
    }

    @Override
    public InstanceSchemaModel getExtend() {

        return null;
    }
}
