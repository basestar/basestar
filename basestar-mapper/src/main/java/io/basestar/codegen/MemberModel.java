package io.basestar.codegen;

import java.util.Map;

public abstract class MemberModel extends Model {

    public MemberModel(final CodegenSettings settings) {

        super(settings);
    }

    public abstract String getName();

    public String getFieldName() {

        return getName();
    }

    public String getAnnotationClassName() {

        return getAnnotationClass().getName();
    }

    protected abstract Class<?> getAnnotationClass();

    public abstract Map<String, Object> getAnnotationValues();

    public abstract TypeModel getType();

    public abstract boolean isRequired();
}
