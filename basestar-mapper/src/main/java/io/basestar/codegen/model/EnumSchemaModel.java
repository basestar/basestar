package io.basestar.codegen.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
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
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.EnumSchema.class, ImmutableMap.of("name", schema.getName()))
        );
    }
}
