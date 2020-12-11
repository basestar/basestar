package io.basestar.mapper;

import io.basestar.mapper.internal.TypeMapper;
import io.basestar.type.PropertyContext;
import io.basestar.type.TypeContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

public interface MappingStrategy extends Serializable {

    String INFER_NAME = "";

    Default DEFAULT = new Default();

    default Name schemaName(final MappingContext context, final String name, final TypeContext type) {

        return name.equals(INFER_NAME) ? schemaName(context, type) : schemaName(context, Name.parse(name));
    }

    default Name schemaName(final MappingContext context, final Name name) {

        return name;
    }

    default List<TypeContext> extend(final TypeContext type) {

        final TypeContext parent = type.superclass();
        if(parent == null || (parent.erasedType() == Object.class)) {
            return Immutable.list();
        } else {
            return Immutable.list(parent);
        }
    }

    default boolean concrete(final TypeContext type) {

        return !type.isAbstract();
    }

    Name schemaName(MappingContext context, TypeContext type);

    TypeMapper typeMapper(MappingContext context, TypeContext type);

    boolean isOptional(PropertyContext property);

    class Default implements MappingStrategy {

        @Override
        public Name schemaName(final MappingContext context, final TypeContext type) {

            return Name.of(type.simpleName());
        }

        @Override
        public TypeMapper typeMapper(final MappingContext context, final TypeContext type) {

            return TypeMapper.fromDefault(context, type);
        }

        @Override
        public boolean isOptional(final PropertyContext property) {

            return property.annotation(Nullable.class) != null;
        }
    }
}
