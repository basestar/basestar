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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.schema.exception.IndexValidationException;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Index
 */

@Getter
public class Index implements Named, Described, Serializable, Extendable {

    private static final int DEFAULT_MAX = 100;

    @Nonnull
    private final String name;

    private final long version;

    @Nullable
    private final String description;

    @Nonnull
    private final List<Path> partition;

    @Nonnull
    private final List<Sort> sort;

    @Nonnull
    private final SortedSet<String> projection;

    @Nonnull
    private final SortedMap<String, Path> over;

    @Nullable
    private final Consistency consistency;

    @Nonnull
    private final Map<String, Object> extensions;

    private final boolean unique;

    private final boolean sparse;

    private final int max;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Described {

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Path> partition;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevSetDeserializer.class)
        private Set<String> projection;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = PathDeserializer.class)
        private Map<String, Path> over;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        private Consistency consistency;

        private Boolean unique;

        private Boolean sparse;

        private Integer max;

        public Index build(final String name) {

            return new Index(this, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Index(final Builder builder, final String name) {

        this.name = name;
        this.version = Nullsafe.option(builder.getVersion(), 1L);
        this.description = builder.getDescription();
        this.partition = Nullsafe.immutableCopy(builder.getPartition());
        this.sort = Nullsafe.immutableCopy(builder.getSort());
        this.projection = Nullsafe.immutableSortedCopy(builder.getProjection());
        this.over = Nullsafe.immutableSortedCopy(builder.getOver());
        this.unique = Nullsafe.option(builder.getUnique(), false);
        this.sparse = Nullsafe.option(builder.getSparse(), false);
        this.consistency = builder.getConsistency();
        this.max = Nullsafe.option(builder.getMax(), DEFAULT_MAX);
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
        if (Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
        for(final Path p : partition) {
            if(p.isEmpty()) {
                throw new IndexValidationException("Partition path cannot be empty");
            }
        }
        for(final Sort s : sort) {
            if(s.getPath().isEmpty()) {
                throw new IndexValidationException("Sort path cannot be empty");
            }
        }
        if(partition.isEmpty() && sort.isEmpty()) {
            throw new IndexValidationException("Must specify partition or sort keys");
        }
        if(isMultiValue()) {
            for(final Sort s : sort) {
                if(over.containsKey(s.getPath().first())) {
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

    public Set<Path> getMultiValuePaths() {

        if(over.isEmpty()) {
            return Collections.emptySet();
        } else {
            return over.entrySet().stream()
                    .flatMap(e -> partition.stream()
                            .filter(v -> v.isChild(Path.of(e.getKey())))
                            .map(v -> e.getValue().with(v.withoutFirst())))
                    .collect(Collectors.toSet());
        }
    }

    @Deprecated
    public Set<Path> requiredExpand(final ObjectSchema schema) {

        final Set<Path> paths = new HashSet<>(partition);
        sort.forEach(v -> paths.add(v.getPath()));
        return schema.requiredExpand(paths);
    }

    public List<Object> readPartition(final Map<String, Object> data) {

        return partition.stream().map(k -> k.apply(data))
                .collect(Collectors.toList());
    }

    public List<Object> readSort(final Map<String, Object> data) {

        return sort.stream().map(k -> k.getPath().apply(data))
                .collect(Collectors.toList());
    }

    public Map<String, Use<?>> projectionSchema(final ObjectSchema schema) {

        final Map<String, Use<?>> result = new HashMap<>();
        if(projection.isEmpty()) {
            schema.getAllProperties()
                    .forEach((name, property) -> result.put(name, property.getType()));
            result.putAll(ObjectSchema.METADATA_SCHEMA);
        } else {
            result.put(Reserved.SCHEMA, UseString.DEFAULT);
            result.put(Reserved.ID, UseString.DEFAULT);
            result.put(Reserved.VERSION, UseInteger.DEFAULT);
        }
        return result;
    }

    public List<Path> resolvePartitionPaths() {

        if(over.isEmpty()) {
            return partition;
        } else {
            return partition.stream()
                    .map(v -> {
                        final Path overPath = over.get(v.first());
                        if(overPath != null) {
                            return overPath.with(v.withoutFirst());
                        } else {
                            return v;
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

//    public List<Use<?>> resolveSortPaths(final ObjectSchema schema) {
//
//        return sort.stream()
//                .map(v -> schema.typeOf(v.getPath()))
//                .collect(Collectors.toList());
//    }

    public Map<String, Object> readProjection(final Map<String, Object> data) {

        if(projection.isEmpty()) {
            return data;
        } else {
            // These properties must be projected
            final Set<String> fullProjection = new HashSet<>(projection);
            fullProjection.add(Reserved.SCHEMA);
            fullProjection.add(Reserved.ID);
            fullProjection.add(Reserved.VERSION);
            partition.forEach(v -> fullProjection.add(v.first()));
            sort.forEach(v -> fullProjection.add(v.getPath().first()));
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

        if(over.isEmpty()) {
            final List<Object> partition = readPartition(data);
            final List<Object> sort = readSort(data);
            if(shouldIndex(partition, sort)) {
                return ImmutableMap.of(Key.of(partition, sort), readProjection(data));
            } else {
                return ImmutableMap.of();
            }
        } else {
            final Map<String, Collection<?>> values = new HashMap<>();
            for (final Map.Entry<String, Path> entry : over.entrySet()) {
                final Path path = entry.getValue();
                final Object value = path.apply(data);
                if (value instanceof Collection<?>) {
                    values.put(entry.getKey(), (Collection<?>) value);
                } else {
                    throw new IllegalStateException("Multi-value index path " + path + " must evaluate to a collection (or null)");
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

    @Data
    public static class Key {

        private final List<Object> partition;

        private final List<Object> sort;

        public static Key of(final List<Object> partition, final List<Object> sort) {

            return new Key(partition, sort);
        }
    }

    public interface Resolver {

        Map<String, Index> getDeclaredIndexes();

        Map<String, Index> getAllIndexes();

        default Index getIndex(final String name, final boolean inherited) {

            if(inherited) {
                return getAllIndexes().get(name);
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
}
