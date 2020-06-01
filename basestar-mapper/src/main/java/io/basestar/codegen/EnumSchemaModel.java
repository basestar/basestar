package io.basestar.codegen;

import io.basestar.schema.EnumSchema;

import java.util.List;

public class EnumSchemaModel extends SchemaModel {

    private final EnumSchema schema;

    public EnumSchemaModel(final CodegenSettings settings, final EnumSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    public List<String> getValues() {

        return schema.getValues();
    }

    @Override
    protected Class<?> getAnnotationClass() {

        return io.basestar.mapper.annotation.EnumSchema.class;
    }
}
