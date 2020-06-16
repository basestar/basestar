package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Schema;
import io.basestar.util.Text;

import java.util.List;

public abstract class SchemaModel extends Model {

    private final Schema<?> schema;

    public SchemaModel(final CodegenSettings settings, final Schema<?> schema) {

        super(settings);
        this.schema = schema;
    }

    public String getClassName() {

        return Text.upperCamel(getName());
    }

    public String getName() {

        return schema.getName();
    }

    public String getDescription() {

        return schema.getDescription();
    }

//    public String getAnnotationClassName() {
//
//        return getAnnotationClass().getName();
//    }
//
//    protected abstract Class<?> getAnnotationClass();
//
//    public Map<String, Object> getAnnotationValues() {
//
//        return ImmutableMap.of("name", schema.getName());
//    }

    public abstract List<AnnotationModel> getAnnotations();
}
