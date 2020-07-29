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

import com.google.common.collect.Maps;
import io.basestar.util.Name;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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

    public LocalDateTime getCreated() {

        return getCreated(backing);
    }

    public Instance setCreated(final LocalDateTime created) {

        setCreated(backing, created);
        return this;
    }

    public LocalDateTime getUpdated() {

        return getUpdated(backing);
    }

    public Instance setUpdated(final LocalDateTime updated) {

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

        return as.cast(backing.get(name));
    }

//    public <T> T getProperty(final String name, final Class<T> cls) {
//
//        return schema.getProperty(backing, name, cls);
//    }
//
//    public <T> Instance setProperty(final String name, final T v) {
//
//        schema.setProperty(backing, name, v);
//        return this;
//    }

    @Override
    public Set<Entry<String, Object>> entrySet() {

        return backing.entrySet();
    }

    public static Name getSchema(final Map<String, Object> data) {

        final String qualifiedName = (String)data.get(Reserved.SCHEMA);
        return qualifiedName == null ? null : Name.parse(qualifiedName);
    }

    public static void setSchema(final Map<String, Object> object, final Name qualifiedName) {

        object.put(Reserved.SCHEMA, qualifiedName == null ? null : qualifiedName.toString());
    }

    public static String getId(final Map<String, Object> object) {

        return (String)object.get(Reserved.ID);
    }

    public static void setId(final Map<String, Object> object, final String id) {

        object.put(Reserved.ID, id);
    }

    public static Long getVersion(final Map<String, Object> object) {

        final Number number = (Number)object.get(Reserved.VERSION);
        return number == null ? null : number.longValue();
    }

    public static void setVersion(final Map<String, Object> object, final Long version) {

        object.put(Reserved.VERSION, version);
    }

    public static String getHash(final Map<String, Object> object) {

        return (String)object.get(Reserved.HASH);
    }

    public static void setHash(final Map<String, Object> object, final String hash) {

        object.put(Reserved.HASH, hash);
    }

    public static LocalDateTime getCreated(final Map<String, Object> object) {

        return (LocalDateTime)object.get(Reserved.CREATED);
    }

    public static void setCreated(final Map<String, Object> object, final LocalDateTime created) {

        object.put(Reserved.CREATED, created);
    }

    public static LocalDateTime getUpdated(final Map<String, Object> object) {

        return (LocalDateTime)object.get(Reserved.UPDATED);
    }

    public static void setUpdated(final Map<String, Object> object, final LocalDateTime updated) {

        object.put(Reserved.UPDATED, updated);
    }

    @SuppressWarnings("unchecked")
    public static Collection<? extends Map<String, Object>> getLink(final Map<String, Object> data, final String link) {

        return (Collection<? extends Map<String, Object>>)data.get(link);
    }
}


