package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.jackson.serde.NameDeserializer;
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
import java.util.*;
import java.util.stream.Stream;

@Getter
@Accessors(chain = true)
public class InterfaceSchema implements ReferableSchema {

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    /**
     * Parent schema, may be another object schema or a struct schema
     */

    @Nullable
    private final List<InterfaceSchema> extend;

    /**
     * Description of the schema
     */

    @Nullable
    private final String description;

    /**
     * Map of property definitions (shares namespace with transients and links)
     */

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    /**
     * Map of property definitions (shares namespace with transients and links)
     */

    @Nonnull
    private final SortedMap<String, Property> properties;

    /**
     * Map of transient definitions (shares namespace with properties and links)
     */

    @Nonnull
    private final SortedMap<String, Transient> declaredTransients;

    /**
     * Map of link definitions (shares namespace with properties and transients)
     */

    @Nonnull
    private final SortedMap<String, Transient> transients;

    /**
     * Map of link definitions (shares namespace with properties and transients)
     */

    @Nonnull
    private final SortedMap<String, Link> declaredLinks;

    /**
     * Map of link definitions (shares namespace with properties and transients)
     */

    @Nonnull
    private final SortedMap<String, Link> links;

    /**
     * Map of index definitions
     */

    @Nonnull
    private final SortedMap<String, Index> declaredIndexes;

    /**
     * Map of index definitions
     */

    @Nonnull
    private final SortedMap<String, Index> indexes;

    /**
     * Map of permissions
     */

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Permission> permissions;

    @Nonnull
    private final List<Constraint> constraints;

    @Nonnull
    private final SortedSet<Name> declaredExpand;

    @Nonnull
    private final SortedSet<Name> expand;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    private final Collection<ReferableSchema> directlyExtended;

    @JsonDeserialize(as = InterfaceSchema.Builder.class)
    public interface Descriptor extends ReferableSchema.Descriptor<InterfaceSchema> {

        String TYPE = "interface";

        @Override
        default String getType() {

            return TYPE;
        }

        interface Self extends ReferableSchema.Descriptor.Self<InterfaceSchema>, Descriptor {

        }

        @Override
        default InterfaceSchema build(final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new InterfaceSchema(this, resolver, version, qualifiedName, slot);
        }

        @Override
        default InterfaceSchema build(final Name qualifiedName) {

            return build(Resolver.Constructing.ANONYMOUS, Version.CURRENT, qualifiedName, Schema.anonymousSlot());
        }

        @Override
        default InterfaceSchema build() {

            return build(Schema.anonymousQualifiedName());
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "id", "history", "properties", "transients", "links", "indexes", "permissions", "extensions"})
    public static class Builder implements ReferableSchema.Builder<Builder, InterfaceSchema>, Descriptor {

        private Long version;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private List<Name> extend;

        @Nullable
        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Transient.Descriptor> transients;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Link.Descriptor> links;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Index.Descriptor> indexes;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<? extends Constraint> constraints;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Descriptor> permissions;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> expand;

        @Nullable
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private InterfaceSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.extend = Immutable.transform(descriptor.getExtend(), resolver::requireInterfaceSchema);
        this.description = descriptor.getDescription();
        this.declaredProperties = Immutable.transformSorted(descriptor.getProperties(), (k, v) -> v.build(resolver, version, qualifiedName.with(k)));
        this.declaredTransients = Immutable.transformSorted(descriptor.getTransients(), (k, v) -> v.build(qualifiedName.with(k)));
        this.declaredLinks = Immutable.transformSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredIndexes = Immutable.transformSorted(descriptor.getIndexes(), (k, v) -> v.build(this, qualifiedName.with(k)));
        this.constraints = Immutable.copy(descriptor.getConstraints());
        this.declaredPermissions = Immutable.transformSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.declaredExpand = Immutable.sortedCopy(descriptor.getExpand());
        this.extensions = Immutable.sortedCopy(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        Stream.of(this.declaredProperties, this.declaredLinks, this.declaredTransients)
                .flatMap(v -> v.keySet().stream()).forEach(k -> {
            if (METADATA_SCHEMA.containsKey(k)) {
                throw new ReservedNameException(k);
            }
        });
        this.properties = Property.extend(extend, declaredProperties);
        this.transients = Transient.extend(extend, declaredTransients);
        this.links = Link.extend(extend, declaredLinks);
        this.indexes = Index.extend(extend, declaredIndexes);
        this.permissions = Permission.extend(extend, declaredPermissions);
        this.expand = LinkableSchema.extendExpand(extend, declaredExpand);

        this.directlyExtended = Immutable.transform(resolver.getExtendedSchemas(qualifiedName), v -> (ReferableSchema)v);
    }

    @Override
    public boolean isConcrete() {

        return false;
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> InterfaceSchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof InterfaceSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }
}
