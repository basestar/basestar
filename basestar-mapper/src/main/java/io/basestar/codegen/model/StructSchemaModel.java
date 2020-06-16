package io.basestar.codegen.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.StructSchema;

import java.util.List;

public class StructSchemaModel extends InstanceSchemaModel {

    private final StructSchema schema;

    public StructSchemaModel(final CodegenSettings settings, final StructSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), javax.validation.Valid.class),
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.StructSchema.class, ImmutableMap.of("name", schema.getName()))
        );
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
