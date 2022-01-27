package io.basestar.schema.test;

import io.basestar.expression.Context;
import io.basestar.schema.Constraint;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.schema.Version;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

public class CustomSchema implements Schema<Object> {

    @Data
    @Accessors(chain = true)
    public static class Builder implements Schema.Builder<Builder, CustomSchema, Object> {

        public static final String TYPE = "custom";

        private String description;

        private Long version;

        private Map<String, Serializable> extensions;

        @Override
        public String getType() {

            return TYPE;
        }

        @Override
        public CustomSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new CustomSchema();
        }
    }

    @Nullable
    @Override
    public String getDescription() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Serializable> getExtensions() {

        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Name getQualifiedName() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Object create(final ValueContext context, final Object value, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public int getSlot() {

        throw new UnsupportedOperationException();
    }

    @Override
    public long getVersion() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Object after) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Type javaType(final Name name) {

        throw new UnsupportedOperationException();
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Descriptor<? extends Schema<Object>, ?> descriptor() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Use<Object> typeOf() {

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(final Object value) {

        throw new UnsupportedOperationException();
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        throw new UnsupportedOperationException();

    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        throw new UnsupportedOperationException();
    }
}
