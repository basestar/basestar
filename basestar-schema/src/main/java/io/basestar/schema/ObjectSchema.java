package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.Widening;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Object Schema
 * <p>
 * Objects are persisted by reference, and may be polymorphic.
 * <p>
 * Objects may contain properties, transients and links, these member types share the same
 * namespace, meaning you cannot define a property and a transient or link with the same name.
 *
 * <strong>Example</strong>
 * <pre>
 * MyObject:
 *   type: object
 *   properties:
 *      myProperty1:
 *          type: string
 * </pre>
 */

@Getter
@Accessors(chain = true)
public class ObjectSchema implements ReferableSchema {

    public static final String ID = ReferableSchema.ID;

    public static final String SCHEMA = ReferableSchema.SCHEMA;

    public static final String CREATED = ReferableSchema.CREATED;

    public static final String UPDATED = ReferableSchema.UPDATED;

    public static final String VERSION = ReferableSchema.VERSION;

    public static final String HASH = ReferableSchema.HASH;

    public static final Name ID_NAME = Name.of(ID);

    public static final SortedMap<String, Use<?>> METADATA_SCHEMA = ReferableSchema.METADATA_SCHEMA;

    public static final SortedMap<String, Use<?>> REF_SCHEMA = ReferableSchema.REF_SCHEMA;

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

    private final List<InterfaceSchema> extend;

    /**
     * Id configuration
     */

    @Nullable
    private final Id id;

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
     * Map of named queries (shares namespace with properties and transients)
     */

    @Nonnull
    private final SortedMap<String, Query> declaredQueries;

    /**
     * Map of named queries (shares namespace with properties and transients)
     */

    @Nonnull
    private final SortedMap<String, Query> queries;

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

    private final boolean readonly;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends ReferableSchema.Descriptor<ObjectSchema> {

        String TYPE = "object";

        @Override
        default String getType() {

            return TYPE;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Id.Descriptor getId();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getReadonly();

        interface Self extends ReferableSchema.Descriptor.Self<ObjectSchema>, Descriptor {

            @Override
            default Id.Descriptor getId() {

                final Id id = self().getId();
                return id == null ? null : id.descriptor();
            }

            @Override
            default Boolean getReadonly() {

                return self().isReadonly();
            }

            @Override
            default History getHistory() {

                return self().getHistory();
            }
        }

        @Override
        default ObjectSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new ObjectSchema(this, resolver, version, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "id", "history", "properties", "transients", "links", "indexes", "permissions", "extensions"})
    public static class Builder implements ReferableSchema.Builder<Builder, ObjectSchema>, Descriptor {

        private Long version;

        @Nullable
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Name> extend;

        @Nullable
        private Id.Builder id;

        @Nullable
        @Deprecated
        private Boolean concrete;

        @Nullable
        private Boolean readonly;

        @Nullable
        private History history;

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

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Query.Descriptor> queries;

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
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private ObjectSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.extend = Immutable.transform(descriptor.getExtend(), resolver::requireInterfaceSchema);
        this.description = descriptor.getDescription();
        this.id = descriptor.getId() == null ? null : descriptor.getId().build(qualifiedName.with(ID));
        this.history = Nullsafe.orDefault(descriptor.getHistory(), History.ENABLED);
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(), (k, v) -> v.build(resolver, version, qualifiedName.with(k)));
        this.properties = Property.extend(extend, declaredProperties);
        final InferenceContext context = InferenceContext.empty()
                .overlay(Reserved.THIS, InferenceContext.from(Immutable.transformValues(this.properties, (k, v) -> v.typeOf())));
        this.declaredTransients = Immutable.transformValuesSorted(descriptor.getTransients(), (k, v) -> v.build(resolver, context, qualifiedName.with(k)));
        this.transients = Transient.extend(extend, declaredTransients);
        this.declaredLinks = Immutable.transformValuesSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.links = Link.extend(extend, declaredLinks);
        this.declaredIndexes = Immutable.transformValuesSorted(descriptor.getIndexes(), (k, v) -> v.build(this, qualifiedName.with(k)));
        this.indexes = Index.extend(extend, declaredIndexes);
        this.declaredQueries = Immutable.transformValuesSorted(descriptor.getQueries(), (k, v) -> v.build(resolver, this, qualifiedName.with(k)));
        this.queries = Query.extend(extend, declaredQueries);
        this.constraints = Immutable.list(descriptor.getConstraints());
        this.declaredPermissions = Immutable.transformValuesSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.permissions = Permission.extend(extend, declaredPermissions);
        this.declaredExpand = Immutable.sortedSet(descriptor.getExpand());
        this.expand = LinkableSchema.extendExpand(extend, declaredExpand);
        this.declaredBucketing = Immutable.list(descriptor.getBucket());
        this.readonly = Nullsafe.orDefault(descriptor.getReadonly());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        final List<String> declaredNames = Stream.of(this.declaredProperties, this.declaredLinks, this.declaredTransients, this.declaredQueries)
                .flatMap(v -> v.keySet().stream())
                .collect(Collectors.toList());
        declaredNames.forEach(k -> {
            if (METADATA_SCHEMA.containsKey(k)) {
                throw new ReservedNameException(k);
            }
        });
        validateFieldNames(declaredNames);
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance before, final Instance after) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        if (id != null) {
            violations.addAll(id.validate(name, Instance.getId(after), context));
        }
        violations.addAll(ReferableSchema.super.validate(context, name, before, after));
        return violations;
    }

    @Override
    public boolean isConcrete() {

        return true;
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> ObjectSchema.this;
    }

    @Override
    public Collection<ReferableSchema> getDirectlyExtended() {

        return Collections.emptySet();
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof ObjectSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    public boolean requiresMigration(final Schema<?> schema, final Widening widening) {

        if (!(schema instanceof ObjectSchema)) {
            return true;
        }
        final ObjectSchema target = (ObjectSchema) schema;
        if (!Objects.equals(getId(), target.getId())) {
            return true;
        }
        if (!Sets.difference(target.getExpand(), getExpand()).isEmpty()) {
            return true;
        }
        for (final Map.Entry<String, ? extends Member> entry : target.getMembers().entrySet()) {
            final Member targetMember = entry.getValue();
            final Member sourceMember = getProperty(entry.getKey(), true);
            if (sourceMember.requiresMigration(targetMember, widening)) {
                return true;
            }
        }
        for (final Map.Entry<String, Index> entry : target.getIndexes().entrySet()) {
            final Index targetIndex = entry.getValue();
            final Index sourceIndex = getIndex(entry.getKey(), true);
            if (sourceIndex != null) {
                if (sourceIndex.requiresMigration(targetIndex)) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }
}
