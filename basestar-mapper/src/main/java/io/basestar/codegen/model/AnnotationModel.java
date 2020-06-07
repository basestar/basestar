package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;

import java.util.Collections;
import java.util.Map;

public class AnnotationModel extends Model {

    private final Class<?> cls;

    private final Map<String, Object> values;

    public AnnotationModel(final CodegenSettings settings, final Class<?> cls, final Map<String, Object> values) {

        super(settings);
        this.cls = cls;
        this.values = values;
    }

    public AnnotationModel(final CodegenSettings settings, final Class<?> cls) {

        this(settings, cls, Collections.emptyMap());
    }

    public String getClassName() {

        return cls.getName();
    }

    public Map<String, Object> getValues() {

        return values;
    }
}
