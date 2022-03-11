package io.basestar.schema.test;

import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.schema.Version;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class CustomSchema implements Schema {

    @Data
    @Accessors(chain = true)
    public static class Builder implements Schema.Builder<Builder, CustomSchema> {

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

    public static Builder builder() {

        return new Builder();
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
    public int getSlot() {

        throw new UnsupportedOperationException();
    }

    @Override
    public long getVersion() {

        throw new UnsupportedOperationException();
    }

    @Override
    public Descriptor<? extends Schema> descriptor() {

        throw new UnsupportedOperationException();
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        throw new UnsupportedOperationException();

    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        throw new UnsupportedOperationException();
    }
}
