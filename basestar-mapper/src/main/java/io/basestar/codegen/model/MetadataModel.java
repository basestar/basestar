package io.basestar.codegen.model;

import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;

import java.util.Map;

public class MetadataModel extends MemberModel {

    private final String name;

    private final Use<?> type;

    public MetadataModel(final CodegenSettings settings, final String name, final Use<?> type) {

        super(settings);
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {

        return name;
    }

    @Override
    protected Class<?> getAnnotationClass() {

        switch (name) {
            case Reserved.ID:
                return io.basestar.mapper.annotation.Id.class;
            case Reserved.VERSION:
                return io.basestar.mapper.annotation.Version.class;
            case Reserved.CREATED:
                return io.basestar.mapper.annotation.Created.class;
            case Reserved.UPDATED:
                return io.basestar.mapper.annotation.Updated.class;
            case Reserved.HASH:
                return io.basestar.mapper.annotation.Hash.class;
            default:
                throw new UnsupportedOperationException("Invalid metadata " + name);
        }
    }

    @Override
    public Map<String, Object> getAnnotationValues() {

        return ImmutableMap.of();
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), type);
    }

    @Override
    public boolean isRequired() {

        return false;
    }
}
