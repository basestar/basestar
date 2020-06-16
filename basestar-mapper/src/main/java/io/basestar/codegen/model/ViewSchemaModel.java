package io.basestar.codegen.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.ViewSchema;

import java.util.List;

public class ViewSchemaModel extends InstanceSchemaModel {

    private final ViewSchema schema;

    public ViewSchemaModel(final CodegenSettings settings, final ViewSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), javax.validation.Valid.class),
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.ViewSchema.class, ImmutableMap.of("name", schema.getName()))
        );
    }

    @Override
    public InstanceSchemaModel getExtend() {

        return null;
    }
}
