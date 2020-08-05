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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

public class Instance extends AbstractMap<String, Object> implements Serializable {

    private final Map<String, Object> backing;

    public Instance() {

        this.backing = Maps.newHashMap();
    }

    public Instance(final Map<String, Object> init) {

        this.backing = Maps.newHashMap(init);
    }

    public Instance(final Instance copy) {

        this.backing = Maps.newHashMap(copy.backing);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Comparator<Map<String, Object>> comparator(final List<Sort> sort) {

        return Sort.comparator(sort, (t, name) -> (Comparable)name.apply(t));
    }

    public static Comparator<Map<String, Object>> comparator(final Sort sort) {

        return comparator(ImmutableList.of(sort));
    }

    public static Comparator<Map<String, Object>> idComparator() {

        return comparator(Sort.asc(ObjectSchema.ID_NAME));
    }

    public Name getSchema() {

        return getSchema(backing);
    }

    public Instance setSchema(final Name schema) {

        setSchema(backing, schema);
        return this;
    }

    public String getId() {

        return getId(backing);
    }

    public Instance setId(final String id) {

        setId(backing, id);
        return this;
    }

    public Instant getCreated() {

        return getCreated(backing);
    }

    public Instance setCreated(final Instant created) {

        setCreated(backing, created);
        return this;
    }

    public Instant getUpdated() {

        return getUpdated(backing);
    }

    public Instance setUpdated(final Instant updated) {

        setUpdated(backing, updated);
        return this;
    }

    public Long getVersion() {

        return getVersion(backing);
    }

    public Instance setVersion(final Long version) {

        setVersion(backing, version);
        return this;
    }

    public String getHash() {

        return getHash(backing);
    }

    public Instance setHash(final String hash) {

        setHash(backing, hash);
        return this;
    }

    public Instance set(final String name, final Object value) {

        backing.put(name, value);
        return this;
    }

    public <T> T get(final String name, final Class<T> as) {

        final Object value = backing.get(name);
        return value == null ? null : as.cast(value);
    }

    public Instance with(final String name, final Object value) {

        return new Instance(with(backing, name, value));
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {

        return backing.entrySet();
    }

    public static Name getSchema(final Map<String, Object> data) {

        final String qualifiedName = (String)data.get(ObjectSchema.SCHEMA);
        return qualifiedName == null ? null : Name.parse(qualifiedName);
    }

    public static void setSchema(final Map<String, Object> object, final Name qualifiedName) {

        object.put(ObjectSchema.SCHEMA, qualifiedName == null ? null : qualifiedName.toString());
    }

    public static String getId(final Map<String, Object> object) {

        return (String)object.get(ObjectSchema.ID);
    }

    public static void setId(final Map<String, Object> object, final String id) {

        object.put(ObjectSchema.ID, id);
    }

    public static Long getVersion(final Map<String, Object> object) {

        final Number number = (Number)object.get(ObjectSchema.VERSION);
        return number == null ? null : number.longValue();
    }

    public static void setVersion(final Map<String, Object> object, final Long version) {

        object.put(ObjectSchema.VERSION, version);
    }

    public static String getHash(final Map<String, Object> object) {

        return (String)object.get(ObjectSchema.HASH);
    }

    public static void setHash(final Map<String, Object> object, final String hash) {

        object.put(ObjectSchema.HASH, hash);
    }

    public static Instant getCreated(final Map<String, Object> object) {

        return (Instant)object.get(ObjectSchema.CREATED);
    }

    public static void setCreated(final Map<String, Object> object, final Instant created) {

        object.put(ObjectSchema.CREATED, created);
    }

    public static Instant getUpdated(final Map<String, Object> object) {

        return (Instant)object.get(ObjectSchema.UPDATED);
    }

    public static void setUpdated(final Map<String, Object> object, final Instant updated) {

        object.put(ObjectSchema.UPDATED, updated);
    }

    public static <T> T get(final Map<String, Object> object, final String name, final Class<T> as) {

        final Object value = object.get(name);
        return value == null ? null : as.cast(value);
    }

    public static void set(final Map<String, Object> object, final String name, final Object value) {

        object.put(name, value);
    }

    @SuppressWarnings("unchecked")
    public static Collection<? extends Map<String, Object>> getLink(final Map<String, Object> data, final String link) {

        return (Collection<? extends Map<String, Object>>)data.get(link);
    }

    public static Map<String, Object> with(final Map<String, Object> object, final String name, final Object value) {

        final Map<String, Object> copy = new HashMap<>(object);
        copy.put(name, value);
        return copy;
    }

    public static Map<String, Object> without(final Map<String, Object> object, final String name) {

        final Map<String, Object> copy = new HashMap<>(object);
        copy.remove(name);
        return copy;
    }
}


