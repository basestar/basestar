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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.IndexValidationException;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import io.basestar.util.*;
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
 * Index
 */

@Getter
public class Index implements Named, Described, Serializable, Extendable {

    private static final int DEFAULT_MAX = 100;

    @Nonnull
    private final ReferableSchema schema;

    @Nonnull
    private final Name qualifiedName;

    private final long version;

    @Nullable
    private final String description;

    @Nonnull
    private final List<Name> partition;

    @Nonnull
    private final List<Sort> sort;

    @Nonnull
    private final SortedSet<String> projection;

    @Nonnull
    private final SortedMap<String, Name> over;

    @Nullable
    private final Consistency consistency;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    private final boolean unique;

    private final boolean sparse;

    private final int max;

    public boolean requiresMigration(final Index target) {

        if(unique != target.isUnique()) {
            return true;
        }
        if(sparse != target.isSparse()) {
            return true;
        }
        if(!Objects.equals(partition, target.getPartition())) {
            return true;
        }
        if(!Objects.equals(sort, target.getSort())) {
            return true;
        }
        if(!Objects.equals(consistency, target.getConsistency())) {
            return true;
        }
        if(!Objects.equals(projection, target.getProjection())) {
            return true;
        }
        return !Objects.equals(over, target.getOver());
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Described, Extendable {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Long getVersion();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Name> getPartition();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getSort();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<String> getProjection();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Name> getOver();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        Consistency getConsistency();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getUnique();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getSparse();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Integer getMax();

        default Index build(final ReferableSchema schema, final Name qualifiedName) {

            return new Index(this, schema, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"version", "description", "partition", "sort", "unique", "sparse", "over", "max", "consistency", "projection", "extensions"})
    public static class Builder implements Descriptor {

        private Long version;

        private String description;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Name> partition;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevSetDeserializer.class)
        private Set<String> projection;

        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Map<String, Name> over;

        private Map<String, Serializable> extensions;

        private Consistency consistency;

        private Boolean unique;

        private Boolean sparse;

        private Integer max;
    }

    public static Builder builder() {

        return new Builder();
    }

    private Index(final Descriptor descriptor, final ReferableSchema schema, final Name qualifiedName) {

        this.schema = schema;
        this.qualifiedName = qualifiedName;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.partition = Immutable.list(descriptor.getPartition());
        this.sort = Immutable.list(descriptor.getSort());
        this.projection = Immutable.sortedSet(descriptor.getProjection());
        this.over = Immutable.sortedMap(descriptor.getOver());
        this.unique = Nullsafe.orDefault(descriptor.getUnique(), false);
        this.sparse = Nullsafe.orDefault(descriptor.getSparse(), false);
        this.consistency = descriptor.getConsistency();
        this.max = Nullsafe.orDefault(descriptor.getMax(), DEFAULT_MAX);
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        for(final Name p : partition) {
            if(p.isEmpty()) {
                throw new IndexValidationException("Partition path cannot be empty");
            }
        }
        for(final Sort s : sort) {
            if(s.getName().isEmpty()) {
                throw new IndexValidationException("Sort path cannot be empty");
            }
        }
        if(partition.isEmpty() && sort.isEmpty()) {
            throw new IndexValidationException("Must specify partition or sort keys");
        }
        if(isMultiValue()) {
            for(final Sort s : sort) {
                if(over.containsKey(s.getName().first())) {
                    throw new IndexValidationException("Multi-value keys cannot be used as sort keys");
                }
            }
            for(final String p : projection) {
                if(over.containsKey(p)) {
                    throw new IndexValidationException("Multi-value keys cannot be used in projection");
                }
            }
        }
        if(unique && !Consistency.ATOMIC.equals(consistency)) {
            throw new IndexValidationException("Unique index must have " + Consistency.ATOMIC + " consistency");
        }
    }

    public Consistency getConsistency(final Consistency defaultValue) {

        if(consistency == null) {
            return defaultValue;
        } else {
            return consistency;
        }
    }

    public boolean isMultiValue() {

        return !over.isEmpty();
    }

    public Set<Name> getMultiValuePaths() {

        if(over.isEmpty()) {
            return Collections.emptySet();
        } else {
            return over.entrySet().stream()
                    .flatMap(e -> partition.stream()
                            .filter(v -> v.isChild(Name.of(e.getKey())))
                            .map(v -> e.getValue().with(v.withoutFirst())))
                    .collect(Collectors.toSet());
        }
    }

    @Deprecated
    public Set<Name> requiredExpand(final ReferableSchema schema) {

        final Set<Name> names = new HashSet<>(partition);
        sort.forEach(v -> names.add(v.getName()));
        return schema.requiredExpand(names);
    }

    public List<Object> readPartition(final Map<String, Object> data) {

        return partition.stream().map(k -> k.get(data))
                .collect(Collectors.toList());
    }

    public List<Object> readSort(final Map<String, Object> data) {

        return sort.stream().map(k -> k.getName().get(data))
                .collect(Collectors.toList());
    }

    public Map<String, Use<?>> projectionSchema(final ReferableSchema schema) {

        final Map<String, Use<?>> result = new HashMap<>();
        if(projection.isEmpty()) {
            schema.getProperties().forEach((name, property) -> result.put(name, property.typeOf()));
            result.putAll(schema.metadataSchema());
        } else {
            final Set<String> members = new HashSet<>(projection);
            resolvePartitionNames().forEach(name -> members.add(name.first()));
            sort.forEach(v -> members.add(v.getName().first()));
            members.forEach(name -> result.put(name, schema.typeOf(Name.of(name))));
            result.put(ReferableSchema.SCHEMA, UseString.DEFAULT);
            result.put(ReferableSchema.ID, UseString.DEFAULT);
            result.put(ReferableSchema.VERSION, UseInteger.DEFAULT);
        }
        return result;
    }

    public List<Name> resolvePartitionNames() {

        if(over.isEmpty()) {
            return partition;
        } else {
            return partition.stream()
                    .map(v -> {
                        final Name overName = over.get(v.first());
                        if(overName != null) {
                            return overName.with(v.withoutFirst());
                        } else {
                            return v;
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

    public Map<String, Object> readProjection(final Map<String, Object> data) {

        if(projection.isEmpty()) {
            return data;
        } else {
            // These properties must be projected
            final Set<String> fullProjection = new HashSet<>(projection);
            fullProjection.add(ReferableSchema.SCHEMA);
            fullProjection.add(ReferableSchema.ID);
            fullProjection.add(ReferableSchema.VERSION);
            resolvePartitionNames().forEach(v -> fullProjection.add(v.first()));
            sort.forEach(v -> fullProjection.add(v.getName().first()));
            final Map<String, Object> result = new HashMap<>();
            fullProjection.forEach(k -> {
                if(data.containsKey(k)) {
                    result.put(k, data.get(k));
                }
            });
            return result;
        }
    }

    private boolean shouldIndex(final List<Object> partition, final List<Object> sort) {

        if(partition.contains(null) || sort.contains(null)) {
            return sparse;
        } else {
            return true;
        }
    }

    public Map<Key, Map<String, Object>> readValues(final Map<String, Object> data) {

        if(data == null) {
            return Collections.emptyMap();
        } else if(over.isEmpty()) {
            final List<Object> partition = readPartition(data);
            final List<Object> sort = readSort(data);
            if(shouldIndex(partition, sort)) {
                return ImmutableMap.of(Key.of(partition, sort), readProjection(data));
            } else {
                return ImmutableMap.of();
            }
        } else {
            final Map<String, Collection<?>> values = new HashMap<>();
            for (final Map.Entry<String, Name> entry : over.entrySet()) {
                final Name name = entry.getValue();
                final Object value = name.get(data);
                if (value instanceof Collection<?>) {
                    values.put(entry.getKey(), (Collection<?>) value);
                } else if(value instanceof Map<?, ?>) {
                    values.put(entry.getKey(), ((Map<?, ?>)value).values());
                } else {
                    throw new IllegalStateException("Multi-value index path " + name + " must evaluate to a collection, a map, or null");
                }
            }
            final Map<Key, Map<String, Object>> records = new HashMap<>();
            final Set<Map<String, Object>> product = product(values, max);
            for(final Map<String, Object> value : product) {
                final Map<String, Object> merged = new HashMap<>(data);
                merged.putAll(value);
                final List<Object> partition = readPartition(merged);
                // NOTE: multi-value cannot be referenced in sort
                final List<Object> sort = readSort(data);
                if(shouldIndex(partition, sort)) {
                    records.put(Key.of(partition, sort), readProjection(data));
                }
            }
            return records;
        }
    }

    private Set<Map<String, Object>> product(final Map<String, Collection<?>> input, final int max) {

        if (input.isEmpty()) {
            return Collections.singleton(Collections.emptyMap());
        } else {
            final Set<Map<String, Object>> results = new HashSet<>();
            final Map.Entry<String, Collection<?>> entryA = input.entrySet().iterator().next();
            final Map<String, Collection<?>> rest = input.entrySet().stream()
                    .filter(entryB -> !entryB.getKey().equals(entryA.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            for (final Object valueA : entryA.getValue()) {
                for (final Map<String, Object> mapB : product(rest, max)) {
                    final Map<String, Object> result = new HashMap<>(mapB);
                    result.put(entryA.getKey(), valueA);
                    results.add(result);
                }
            }
            if (results.size() > max) {
                throw new IllegalStateException("Index product returned too many results (max= " + max + ")");
            }
            return results;
        }
    }

    public Index extend(final Index ext) {

        return ext;
    }

    public static SortedMap<String, Index> extend(final Map<String, Index> base, final Map<String, Index> ext) {

        return Immutable.sortedMerge(base, ext, Index::extend);
    }

    public static SortedMap<String, Index> extend(final Collection<? extends Resolver> base, final Map<String, Index> ext) {

        return Immutable.sortedMap(Stream.concat(
                base.stream().map(Resolver::getIndexes),
                Stream.of(ext)
        ).reduce(Index::extend).orElse(Collections.emptyMap()));
    }

    /**
     * Contains the matched partition and sort values for a given record
     */

    @Data
    public static class Key {

        private final List<Object> partition;

        private final List<Object> sort;

        public static Key of(final List<?> partition, final List<?> sort) {

            return new Key(Immutable.list(partition), Immutable.list(sort));
        }

        public Binary binary() {

            return new Binary(BinaryKey.from(partition), BinaryKey.from(sort));
        }

        /**
         * Standardized binary encoding of the key (cannot be trivially converted back to Key)
         */

        @Data
        public static class Binary {

            private final BinaryKey partition;

            private final BinaryKey sort;

            public static Binary of(final BinaryKey partition, final BinaryKey sort) {

                return new Binary(partition, sort);
            }
        }
    }

    public interface Resolver {

        interface Descriptor {

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, Index.Descriptor> getIndexes();
        }

        interface Builder<B extends Builder<B>> extends Descriptor {

            default B setIndex(final String name, final Index.Descriptor v) {

                return setIndexes(Immutable.put(getIndexes(), name, v));
            }

            B setIndexes(Map<String, Index.Descriptor> vs);
        }

        Map<String, Index> getDeclaredIndexes();

        Map<String, Index> getIndexes();

        default Map<String, Index.Descriptor> describeDeclaredIndexes() {

            return getDeclaredIndexes().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().descriptor()
            ));
        }

        default Index getIndex(final String name, final boolean inherited) {

            if(inherited) {
                return getIndexes().get(name);
            } else {
                return getDeclaredIndexes().get(name);
            }
        }

        default Index requireIndex(final String name, final boolean inherited) {

            final Index result = getIndex(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }

    public Descriptor descriptor() {

        return new Descriptor() {

            @Override
            public Map<String, Serializable> getExtensions() {

                return extensions;
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }

            @Override
            public Long getVersion() {

                return version;
            }

            @Override
            public List<Name> getPartition() {

                return partition;
            }

            @Override
            public List<Sort> getSort() {

                return sort;
            }

            @Override
            public Set<String> getProjection() {

                return projection;
            }

            @Override
            public Map<String, Name> getOver() {

                return over;
            }

            @Override
            public Consistency getConsistency() {

                return consistency;
            }

            @Override
            public Boolean getUnique() {

                return unique;
            }

            @Override
            public Boolean getSparse() {

                return sparse;
            }

            @Override
            public Integer getMax() {

                return max;
            }
        };
    }

    public Diff diff(final Map<String, Object> before,
                     final Map<String, Object> after) {

        return Diff.from(readValues(before), readValues(after));
    }

    @Data
    public static class Diff {

        private final Map<Key, Map<String, Object>> create;

        private final Map<Key, Map<String, Object>> update;

        private final Set<Key> delete;

        public static Diff from(final Map<Key, Map<String, Object>> before,
                                final Map<Key, Map<String, Object>> after) {

            return new Diff(
                    Sets.difference(after.keySet(), before.keySet()).stream()
                            .collect(Collectors.toMap(k -> k, after::get)),
                    Sets.intersection(after.keySet(), before.keySet()).stream()
                            .collect(Collectors.toMap(k -> k, after::get)),
                    Sets.difference(before.keySet(), after.keySet())
            );
        }
    }
}
