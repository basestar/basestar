package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Getter
@Accessors(chain = true)
public class SequenceSchema implements Schema {

    @Nonnull
    private final Name qualifiedName;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    private final int slot;

    private final Expression format;

    private final Long start;

    /**
     * Text description
     */

    @Nullable
    private final String description;

    @Nonnull
    private final Map<String, Serializable> extensions;

    public String format(final Long value) {

        if (format == null) {
            return value.toString();
        } else {
            return format.evaluateAs(String.class, Context.init(Immutable.map(Reserved.VALUE, value)));
        }
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Schema.Descriptor<SequenceSchema> {

        String TYPE = "sequence";

        @Override
        default String getType() {

            return TYPE;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonSerialize(using = ToStringSerializer.class)
        Expression getFormat();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Long getStart();

        interface Self extends Schema.Descriptor.Self<SequenceSchema>, SequenceSchema.Descriptor {

            @Override
            default Expression getFormat() {

                return self().getFormat();
            }

            @Override
            default Long getStart() {

                return self().getStart();
            }
        }

        @Override
        default SequenceSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new SequenceSchema(this, resolver, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonPropertyOrder({"type", "description", "version", "values", "extensions"})
    public static class Builder implements Schema.Builder<SequenceSchema.Builder, SequenceSchema>, SequenceSchema.Descriptor {

        private Long version;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private String description;

        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression format;

        private Long start;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private SequenceSchema(final Descriptor descriptor, final Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.format = descriptor.getFormat();
        this.start = descriptor.getStart();
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

    }

    public Long getEffectiveStart() {

        return Nullsafe.orDefault(start, 0L);
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> SequenceSchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof SequenceSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }
}
