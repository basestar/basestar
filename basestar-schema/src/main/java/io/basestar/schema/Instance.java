package io.basestar.schema;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.AbstractMap;
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

    public String getSchema() {

        return getSchema(backing);
    }

    public Instance setSchema(final String schema) {

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

    @SuppressWarnings("unchecked")
    public <T> T get(final String name, final T value) {

        return (T)backing.get(name);
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

    public static String getSchema(final Map<String, Object> data) {

        return (String)data.get(Reserved.SCHEMA);
    }

    public static void setSchema(final Map<String, Object> object, final String schema) {

        object.put(Reserved.SCHEMA, schema);
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

        final String str = (String)object.get(Reserved.CREATED);
        return str == null ? null : LocalDateTime.parse(str);
    }

    public static void setCreated(final Map<String, Object> object, final LocalDateTime created) {

        object.put(Reserved.CREATED, created == null ? null : created.toString());
    }

    public static LocalDateTime getUpdated(final Map<String, Object> object) {

        final String str = (String)object.get(Reserved.UPDATED);
        return str == null ? null : LocalDateTime.parse(str);
    }

    public static void setUpdated(final Map<String, Object> object, final LocalDateTime updated) {

        object.put(Reserved.UPDATED, updated == null ? null : updated.toString());
    }
}


