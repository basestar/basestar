package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataOutput;
import java.io.IOException;
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
     * History configuration
     */

    @Nonnull
    private final History history;

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
    private final List<Bucketing> declaredBucketing;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    private final Namespace namespace;

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
        default InterfaceSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new InterfaceSchema(this, namespace, resolver, version, qualifiedName, slot);
        }

        @Override
        default InterfaceSchema build(final Name qualifiedName) {

            return build(null, Resolver.Constructing.ANONYMOUS, Version.CURRENT, qualifiedName, Schema.anonymousSlot());
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
        @JsonDeserialize(using = AbbrevListDeserializer.class)
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
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Bucketing> bucket;

        @Nullable
        private History history;

        @Nullable
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private InterfaceSchema(final Descriptor descriptor, final Namespace namespace, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.namespace = namespace;
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.extend = Immutable.transform(descriptor.getExtend(), resolver::requireInterfaceSchema);
        this.history = Nullsafe.orDefault(descriptor.getHistory(), History.ENABLED);
        this.description = descriptor.getDescription();
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(), (k, v) -> v.build(resolver, version, qualifiedName.with(k)));
        this.properties = Property.extend(extend, declaredProperties);
        final InferenceContext context = InferenceContext.from(Immutable.transformValues(this.properties, (k, v) -> v.typeOf()));
        this.declaredTransients = Immutable.transformValuesSorted(descriptor.getTransients(), (k, v) -> v.build(resolver, context, qualifiedName.with(k)));
        this.transients = Transient.extend(extend, declaredTransients);
        this.declaredLinks = Immutable.transformValuesSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.links = Link.extend(extend, declaredLinks);
        this.declaredIndexes = Immutable.transformValuesSorted(descriptor.getIndexes(), (k, v) -> v.build(this, qualifiedName.with(k)));
        this.indexes = Index.extend(extend, declaredIndexes);
        this.constraints = Immutable.list(descriptor.getConstraints());
        this.declaredPermissions = Immutable.transformValuesSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.permissions = Permission.extend(extend, declaredPermissions);
        this.declaredExpand = Immutable.sortedSet(descriptor.getExpand());
        this.expand = LinkableSchema.extendExpand(extend, declaredExpand);
        this.declaredBucketing = Immutable.list(descriptor.getBucket());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        Stream.of(this.declaredProperties, this.declaredLinks, this.declaredTransients)
                .flatMap(v -> v.keySet().stream()).forEach(k -> {
            if (METADATA_SCHEMA.containsKey(k)) {
                throw new ReservedNameException(k);
            }
        });
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
    public Collection<ReferableSchema> getDirectlyExtended() {

        return Immutable.transform(namespace.getExtendedSchemas(qualifiedName), v -> (ReferableSchema) v);
    }

    @Override
    public InstanceSchema resolveExtended(final Name name) {

        if(getQualifiedName().equals(name)) {
            return this;
        } else {
            final Optional<ReferableSchema> schema = getIndirectlyExtended().stream()
                    .filter(v -> v.getQualifiedName().equals(name))
                    .findFirst();
            if(schema.isPresent()) {
                return schema.get();
            } else {
                throw new IllegalStateException(name + " is not a valid subtype of " + getQualifiedName());
            }
        }
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof InterfaceSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    private ReferableSchema getExtendedOrThis(final Name schemaName) {

        if (schemaName == null || schemaName.equals(getQualifiedName())) {
            return this;
        } else {
            return getIndirectlyExtended().stream()
                    .filter(v -> v.getQualifiedName().equals(schemaName))
                    .findFirst().orElse(this);
        }
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance before, final Instance after) {

        final ReferableSchema resolvedSchema = getExtendedOrThis(Instance.getSchema(after));
        if (resolvedSchema == this) {
            return ReferableSchema.super.validate(context, name, before, after);
        } else {
            return resolvedSchema.validate(context, name, before, after);
        }
    }

    @Override
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        final ReferableSchema resolvedSchema = getExtendedOrThis(Instance.getSchema(value));
        if (resolvedSchema == this) {
            return ReferableSchema.super.create(context, value, expand);
        } else {
            return resolvedSchema.create(context, value, expand);
        }
    }

    @Override
    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        final ReferableSchema resolvedSchema = getExtendedOrThis(Instance.getSchema(object));
        if (resolvedSchema == this) {
            ReferableSchema.super.serialize(object, out);
        } else {
            resolvedSchema.serialize(object, out);
        }
    }

    @Override
    public Instance evaluateProperties(final Context context, final Map<String, Object> object, final Set<Name> expand) {

        final ReferableSchema resolvedSchema = getExtendedOrThis(Instance.getSchema(object));
        if (resolvedSchema == this) {
            return ReferableSchema.super.evaluateProperties(context, object, expand);
        } else {
            return resolvedSchema.evaluateProperties(context, object, expand);
        }
    }

    @Override
    public Instance evaluateTransients(final Context context, final Instance object, final Set<Name> expand) {

        final ReferableSchema resolvedSchema = getExtendedOrThis(Instance.getSchema(object));
        if (resolvedSchema == this) {
            return ReferableSchema.super.evaluateTransients(context, object, expand);
        } else {
            return resolvedSchema.evaluateTransients(context, object, expand);
        }
    }
}