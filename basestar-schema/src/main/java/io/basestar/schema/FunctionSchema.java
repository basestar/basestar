package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.expression.Context;
import io.basestar.schema.from.From;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(chain = true)
public class FunctionSchema implements CallableSchema {

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    /**
     * Description of the schema
     */

    @Nullable
    private final String description;

    @Nonnull
    private final String language;

    @Nonnull
    private final List<Argument> arguments;

    @Nonnull
    private final Use<?> returns;

    @Nonnull
    private final String definition;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @Nonnull
    private final Map<String, From> using;

    public static Builder builder() {

        return new Builder();
    }

    public FunctionSchema(final Descriptor descriptor, final Resolver.Constructing resolver, final Version version, @Nonnull final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.language = Nullsafe.require(descriptor.getLanguage());
        this.arguments = Immutable.transform(descriptor.getArguments(), v -> v.build(resolver));
        this.returns = Nullsafe.require(descriptor.getReturns()).resolve(resolver);
        this.definition = Nullsafe.require(descriptor.getDefinition());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        this.using = Immutable.transformValues(descriptor.getUsing(), (k, v) -> v.build(resolver, Context.init()));
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends CallableSchema.Descriptor<FunctionSchema> {

        String TYPE = "function";

        @Override
        default String getType() {

            return TYPE;
        }

        @Override
        default FunctionSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new FunctionSchema(this, resolver, version, qualifiedName, slot);
        }

        interface Self extends CallableSchema.Descriptor.Self<FunctionSchema>, Descriptor {

        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "language", "arguments", "returns", "definition", "extensions"})
    public static class Builder implements CallableSchema.Builder<Builder, FunctionSchema>, FunctionSchema.Descriptor {

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private String language;

        @Nullable
        private List<Argument.Descriptor> arguments;

        @Nullable
        private Use<?> returns;

        @Nullable
        private String definition;

        @Nullable
        private Map<String, Serializable> extensions;

        @Nullable
        private Map<String, From.Descriptor> using;
    }

    @Override
    public FunctionSchema.Descriptor descriptor() {

        return (Descriptor.Self) () -> FunctionSchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof FunctionSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }
}