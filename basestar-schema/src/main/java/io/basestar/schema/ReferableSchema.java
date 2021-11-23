package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.schema.use.*;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ReferableSchema extends LinkableSchema, Index.Resolver, Transient.Resolver {

    String ID = "id";

    String SCHEMA = "schema";

    String CREATED = "created";

    String UPDATED = "updated";

    String VERSION = "version";

    String HASH = "hash";

    SortedMap<String, Use<?>> METADATA_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(ID, UseString.DEFAULT)
            .put(SCHEMA, UseString.DEFAULT)
            .put(VERSION, UseInteger.DEFAULT)
            .put(CREATED, UseDateTime.DEFAULT)
            .put(UPDATED, UseDateTime.DEFAULT)
            .put(HASH, UseString.DEFAULT)
//            .put(__ID, UseBinary.DEFAULT)
            .build();

    SortedMap<String, Use<?>> REF_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(ID, UseString.DEFAULT)
            .build();

    SortedMap<String, Use<?>> VERSIONED_REF_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(ID, UseString.DEFAULT)
            .put(VERSION, UseInteger.DEFAULT)
            .build();

    static boolean isResolved(final Map<String, Object> instance) {

        return instance != null && instance.containsKey(ReferableSchema.SCHEMA);
    }

    interface Descriptor<S extends ReferableSchema> extends LinkableSchema.Descriptor<S>, Transient.Resolver.Descriptor, Index.Resolver.Descriptor {

        @Nullable
        History getHistory();

        @JsonDeserialize(using = AbbrevListDeserializer.class)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Name> getExtend();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<? extends Constraint> getConstraints();

        interface Self<S extends ReferableSchema> extends LinkableSchema.Descriptor.Self<S>, Descriptor<S> {

            @Override
            default List<Name> getExtend() {

                final List<? extends ReferableSchema> extend = self().getExtend();
                return Immutable.transform(extend, Named::getQualifiedName);
            }

            @Override
            default History getHistory() {

                return self().getHistory();
            }

            @Override
            default Map<String, Transient.Descriptor> getTransients() {

                return self().describeDeclaredTransients();
            }

            @Override
            default Map<String, Index.Descriptor> getIndexes() {

                return self().describeDeclaredIndexes();
            }

            @Override
            default List<? extends Constraint> getConstraints() {

                return self().getConstraints();
            }
        }
    }

    interface Builder<B extends Builder<B, S>, S extends ReferableSchema> extends LinkableSchema.Builder<B, S>, Descriptor<S>, Transient.Resolver.Builder<B>, Index.Resolver.Builder<B> {

    }

    History getHistory();

    List<? extends ReferableSchema> getExtend();

    List<? extends Constraint> getConstraints();

    @Override
    default List<Sort> sort() {

        return Immutable.list(Sort.asc(Name.of(id())));
    }

    @Override
    Descriptor<? extends ReferableSchema> descriptor();

    default boolean isOrExtending(final Name name) {

        if (name.equals(this.getQualifiedName())) {
            return true;
        } else {
            return isExtending(name);
        }
    }

    default boolean isExtending(final Name name) {

        for(final ReferableSchema extend : getExtend()) {
            if (extend.isOrExtending(name)) {
                return true;
            }
        }
        return false;
    }

    Collection<ReferableSchema> getDirectlyExtended();

    default Collection<ReferableSchema> getIndirectlyExtended() {

        final Set<ReferableSchema> results = new HashSet<>();
        for(final ReferableSchema schema : getDirectlyExtended()) {
            results.add(schema);
            results.addAll(schema.getIndirectlyExtended());
        }
        return results;
    }

    default Collection<ObjectSchema> getConcreteExtended() {

        final Set<ObjectSchema> results = new HashSet<>();
        getIndirectlyExtended().forEach(schema -> {
            if(schema instanceof ObjectSchema) {
                results.add((ObjectSchema)schema);
            }
        });
        return results;
    }

    default Set<ReferableSchema> getIndirectExtend() {

        final Set<ReferableSchema> results = new HashSet<>();
        getExtend().forEach(schema -> {
            results.add(schema);
            results.addAll(schema.getIndirectExtend());
        });
        return results;
    }

    default void validateObject(final String id, final Instance after) {

        if(!id.equals(Instance.getId(after))) {
            throw new IllegalStateException("Instance validation failed: id mismatch");
        }
        final Name schemaName = Instance.getSchema(after);
        if(schemaName == null || !isOrExtending(schemaName)) {
            throw new IllegalStateException("Instance validation failed: schema mismatch");
        }
    }

    default void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        final Name schema = Instance.getSchema(object);
        final String id = Instance.getId(object);
        final Long version = Instance.getVersion(object);
        final Instant created = Instance.getCreated(object);
        final Instant updated = Instance.getUpdated(object);
        final String hash = Instance.getHash(object);
        UseString.DEFAULT.serialize(schema == null ? null : schema.toString(), out);
        UseString.DEFAULT.serialize(id, out);
        UseInteger.DEFAULT.serialize(version, out);
        UseDateTime.DEFAULT.serialize(created, out);
        UseDateTime.DEFAULT.serialize(updated, out);
        UseString.DEFAULT.serialize(hash, out);
        serializeProperties(object, out);
    }

    static Instance deserialize(final DataInput in) throws IOException {

        final String schema = Use.deserializeAny(in);
        final String id = Use.deserializeAny(in);
        final Long version = Use.deserializeAny(in);
        final Instant created = Use.deserializeAny(in);
        final Instant updated = Use.deserializeAny(in);
        final String hash = Use.deserializeAny(in);

        final Map<String, Object> data = new HashMap<>(InstanceSchema.deserializeProperties(in));
        Instance.setSchema(data, schema == null ? null : Name.parse(schema));
        Instance.setId(data, id);
        Instance.setVersion(data, version);
        Instance.setCreated(data, created);
        Instance.setUpdated(data, updated);
        Instance.setHash(data, hash);
        return new Instance(data);
    }

    static Instance ref(final String key) {

        return new Instance(ImmutableMap.of(
                ID, key
        ));
    }

    static Instance versionedRef(final String key, final Long version) {

        return new Instance(ImmutableMap.of(
                ID, key,
                VERSION, version
        ));
    }

    static Map<String, Use<?>> refSchema(final boolean versioned) {

        return versioned ? VERSIONED_REF_SCHEMA : REF_SCHEMA;
    }

    @Override
    default SortedMap<String, Use<?>> metadataSchema() {

        return METADATA_SCHEMA;
    }

    @Override
    default UseRef typeOf() {

        return new UseRef(this);
    }

    @Override
    default String id() {

        return ID;
    }

    @Override
    default String id(final Map<String, Object> data) {

        return Instance.getId(data);
    }

    @Override
    default Use<?> typeOfId() {

        return UseString.DEFAULT;
    }

    @Override
    default Map<String, ? extends Member> getDeclaredMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(getDeclaredProperties());
        members.putAll(getDeclaredTransients());
        members.putAll(getDeclaredLinks());
        members.putAll(getDeclaredQueries());
        return members;
    }

    @Override
    default Map<String, ? extends Member> getMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(getProperties());
        members.putAll(getTransients());
        members.putAll(getLinks());
        members.putAll(getQueries());
        return members;
    }

    @Override
    default Member getMember(final String name, final boolean inherited) {

        final Property property = getProperty(name, inherited);
        if (property != null) {
            return property;
        }
        final Link link = getLink(name, inherited);
        if (link != null) {
            return link;
        }
        return getTransient(name, inherited);
    }

    @Override
    default Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        if (value == null) {
            return null;
        }
        final Name schemaName = Instance.getSchema(value);
        final HashMap<String, Object> result = new HashMap<>(readProperties(context, value, expand));
        result.putAll(readMeta(context, value));
        if (schemaName == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if (Instance.getHash(result) == null) {
            Instance.setHash(result, hash(result));
        }
//        final String id = Instance.getId(result);
//        if(id != null) {
//            result.put(__ID, BinaryKey.from(ImmutableList.of(id)));
//        }
        for (final Map.Entry<String, Object> entry : value.entrySet()) {
            if (entry.getKey().startsWith(Reserved.META_PREFIX)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        if (expand != null && !expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            Stream.of(getLinks(), getTransients()).forEach(members -> members.forEach((name, link) -> {
                if (value.containsKey(name)) {
                    result.put(name, link.create(context, value.get(name), branches.get(name)));
                }
            }));
        }
        return new Instance(result);
    }

    static Map<String, Object> copyMeta(final Map<String, Object> source) {

        final Map<String, Object> target = new HashMap<>();
        copyMeta(source, target);
        return Collections.unmodifiableMap(target);
    }

    static void copyMeta(final Map<String, Object> source, final Map<String, Object> target) {

        source.forEach((k, v) -> {
            if (METADATA_SCHEMA.containsKey(k) || Reserved.isMeta(k)) {
                target.put(k, v);
            }
        });
    }

    default Instance evaluateProperties(final Context context, final Map<String, Object> object, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Context thisContext = context.with(VAR_THIS, object);
        final HashMap<String, Object> result = new HashMap<>();
        getProperties().forEach((k, v) -> result.put(k, v.evaluate(thisContext, branches.get(k), object.get(k))));
        copyMeta(object, result);
        result.put(HASH, hash(result));
        // Links deliberately not copied, this is only used to prepare an instance for write.
        return new Instance(result);
    }

    @Override
    default Set<Constraint.Violation> validate(final Context context, final Name name, final Instance before, final Instance after) {

        final Set<Constraint.Violation> violations = new HashSet<>();

        violations.addAll(this.getConstraints().stream()
                .flatMap(v -> v.violations(new UseRef(this), context, name, after).stream())
                .collect(Collectors.toSet()));

        violations.addAll(LinkableSchema.super.validate(context, name, before, after));

        return violations;
    }

    @Override
    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        final Name qualifiedName = getQualifiedName();
        final List<? extends ReferableSchema> extend = getExtend();
        if (!out.containsKey(qualifiedName)) {
            extend.forEach(ex -> ex.collectDependencies(expand, out));
            out.put(qualifiedName, this);
            final Map<String, Set<Name>> branches = Name.branch(expand);
            getDeclaredProperties().forEach((k, v) -> v.collectDependencies(branches.get(k), out));
            Stream.of(getDeclaredLinks(), getDeclaredTransients()).forEach(members -> members.forEach((k, v) -> {
                final Set<Name> branch = branches.get(k);
                if (branch != null) {
                    v.collectDependencies(branch, out);
                }
            }));
        }
    }
}
