package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;

import java.util.List;

public abstract class MemberModel extends Model {

    public MemberModel(final CodegenSettings settings) {

        super(settings);
    }

    public abstract String getName();

    public String getFieldName() {

        return getName();
    }

    public abstract List<AnnotationModel> getAnnotations();

    public abstract TypeModel getType();

    public abstract boolean isRequired();
}
