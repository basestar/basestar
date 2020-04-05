package io.basestar.schema.use;


import com.google.common.collect.Multimap;
import io.basestar.schema.*;
import io.basestar.schema.exception.MissingTypeException;
import io.basestar.util.Path;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

@Data
public class UseNamed implements Use<Object> {

    private final String name;

    private final Object config;

    public static UseNamed from(final String name, final Object config) {

        return new UseNamed(name, config);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Use<?> resolve(final Schema.Resolver resolver) {

        final Schema schema = resolver.requireSchema(name);
        if(schema instanceof EnumSchema) {
            return UseEnum.from((EnumSchema) schema, config);
        } else if(schema instanceof StructSchema) {
            return UseStruct.from((StructSchema) schema, config);
        } else if(schema instanceof ObjectSchema) {
            return UseObject.from((ObjectSchema) schema, config);
        } else {
            throw new MissingTypeException(name);
        }
    }

    @Override
    public Object create(final Object value) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Code code() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Use<?> typeOf(final Path path) {

        throw new UnsupportedOperationException();
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        throw new UnsupportedOperationException();
//    }

    @Override
    @Deprecated
    public Set<Path> requireExpand(final Set<Path> paths) {

        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Multimap<Path, Instance> refs(final Object value) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Object toJson() {

        return name;
    }

    @Override
    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public void serializeValue(final Object value, final DataOutput out) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Object deserializeValue(final DataInput in) throws IOException {

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {

        return name;
    }
}
