package io.basestar.mapper.internal;

import io.basestar.mapper.MappingContext;
import io.basestar.mapper.SchemaMapper;
import io.basestar.schema.Schema;
import io.basestar.type.TypeContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EnumSchemaMapper<T extends Enum<?>> implements SchemaMapper<T, String> {

    private final String name;

    private final T[] constants;

    public EnumSchemaMapper(final MappingContext context, final String name, final TypeContext type) {

        this.name = name;
        this.constants = type.enumConstants();
    }

    @Override
    public String name() {

        return name;
    }

    @Override
    public Schema.Builder<String> schema() {

        final List<String> values = Arrays.stream(constants).map(Enum::name).collect(Collectors.toList());
        return io.basestar.schema.EnumSchema.builder()
                .setValues(values);
    }

    @Override
    public T marshall(final Object source) {

        if(source == null) {
            return null;
        } else if(source instanceof String) {
            final String value = (String)source;
            return Arrays.stream(constants)
                    .filter(v -> v.name().equalsIgnoreCase(value))
                    .findFirst().orElse(null);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String unmarshall(final T value) {

        return value == null ? null : value.name();
    }

    @Override
    public Set<Class<?>> dependencies() {

        return Collections.emptySet();
    }
}
