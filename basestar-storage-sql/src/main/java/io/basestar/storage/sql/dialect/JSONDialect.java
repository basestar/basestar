package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Instance;
import io.basestar.schema.Property;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.use.*;
import io.basestar.storage.sql.SQLDialect;
import io.basestar.storage.sql.resolver.FieldResolver;
import io.basestar.storage.sql.resolver.ValueResolver;
import io.basestar.util.Bytes;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@Slf4j
public abstract class JSONDialect implements SQLDialect {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    protected abstract boolean isJsonEscaped();

    protected DataType<?> jsonType() {

        return SQLDataType.JSON;
    }

    @Override
    public DataType<?> stringType(final UseString type) {

        return SQLDataType.LONGVARCHAR;
    }

    @Override
    public <T> DataType<?> arrayType(final UseArray<T> type) {

        return jsonType();
    }

    @Override
    public <T> DataType<?> setType(final UseSet<T> type) {

        return jsonType();
    }

    @Override
    public <T> DataType<?> pageType(final UsePage<T> type) {

        return jsonType();
    }

    @Override
    public <T> DataType<?> mapType(final UseMap<T> type) {

        return jsonType();
    }

    @Override
    public DataType<?> structType(final UseStruct type) {

        return jsonType();
    }

    @Override
    public DataType<?> viewType(final UseView type) {

        return jsonType();
    }

    @Override
    public DataType<?> refType(final UseRef type) {

        return stringType(UseString.DEFAULT);
    }

    @Override
    public DataType<?> binaryType(final UseBinary type) {

        return stringType(UseString.DEFAULT);
    }

    @Override
    public DataType<?> anyType(final UseAny type) {

        return jsonType();
    }

    @Override
    public <T> Map<Field<?>, SelectField<?>> arrayToSQLValues(final UseArray<T> type, final FieldResolver field, final List<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public Map<Field<?>, SelectField<?>> dateToSQLValues(final UseDate type, final FieldResolver field, final LocalDate value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, DSL.val(ISO8601.toSqlDate(value)))).orElseGet(ImmutableMap::of);
    }

    @Override
    public Map<Field<?>, SelectField<?>> dateTimeToSQLValues(final UseDateTime type, final FieldResolver field, final Instant value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, DSL.val(ISO8601.toSqlTimestamp(value)))).orElseGet(ImmutableMap::of);
    }

    @Override
    public <T> Map<Field<?>, SelectField<?>> pageToSQLValues(final UsePage<T> type, final FieldResolver field, final Page<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public <T> Map<Field<?>, SelectField<?>> setToSQLValues(final UseSet<T> type, final FieldResolver field, final Set<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public <T> Map<Field<?>, SelectField<?>> mapToSQLValues(final UseMap<T> type, final FieldResolver field, final Map<String, T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public Map<Field<?>, SelectField<?>> structToSQLValues(final UseStruct type, final FieldResolver field, final Instance value) {

        final Map<Field<?>, SelectField<?>> fields = new HashMap<>();
        for (final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
            final FieldResolver column = field.resolver(Name.of(entry.getKey()));
            fields.putAll(toSQLValues(entry.getValue().typeOf(), column, value.get(entry.getKey())));
        }
        return fields;
    }

    @Override
    public Map<Field<?>, SelectField<?>> viewToSQLValues(final UseView type, final FieldResolver field, final Instance value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public Map<Field<?>, SelectField<?>> refToSQLValues(final UseRef type, final FieldResolver field, final Instance value) {

        final Map<Field<?>, SelectField<?>> fields = new HashMap<>(toSQLValues(UseString.DEFAULT, field.resolver(Name.of(ReferableSchema.ID)), value.getId()));
        if (type.isVersioned()) {
            fields.putAll(toSQLValues(UseInteger.DEFAULT, field.resolver(Name.of(ReferableSchema.VERSION)), value.getVersion()));

        }
        return fields;
    }

    @Override
    public Map<Field<?>, SelectField<?>> binaryToSQLValues(final UseBinary type, final FieldResolver field, final Bytes value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, DSL.val(value.toBase64()))).orElseGet(ImmutableMap::of);
    }

    @Override
    public Map<Field<?>, SelectField<?>> anyToSQLValues(final UseAny type, final FieldResolver field, final Object value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, toJson(value))).orElseGet(ImmutableMap::of);
    }

    @Override
    public <T> List<T> arrayFromSQLValue(final UseArray<T> type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            final Collection<?> results = fromJson(v, new TypeReference<Collection<?>>() {
            });
            return type.create(results);
        }
    }

    @Override
    public <T> Page<T> pageFromSQLValue(final UsePage<T> type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            final Collection<?> results = fromJson(v, new TypeReference<Collection<?>>() {
            });
            return type.create(results);
        }
    }

    @Override
    public <T> Set<T> setFromSQLValue(final UseSet<T> type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            final Collection<?> results = fromJson(v, new TypeReference<Collection<?>>() {
            });
            return type.create(results);
        }
    }

    @Override
    public <T> Map<String, T> mapFromSQLValue(final UseMap<T> type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            return type.create(fromJson(v, new TypeReference<Map<String, ?>>() {
            }));
        }
    }

    @Override
    public Instance structFromSQLValue(final UseStruct type, final ValueResolver value) {

        final Map<String, Object> result = new HashMap<>();
        for (final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
            result.put(entry.getKey(), fromSQLValue(entry.getValue().typeOf(), value.resolver(Name.of(entry.getKey()))));
        }
        return new Instance(result);
    }

    @Override
    public Instance viewFromSQLValue(final UseView type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            return type.create(fromJson(v, new TypeReference<Map<String, Object>>() {
            }));
        }
    }

    @Override
    public Instance refFromSQLValue(final UseRef type, final ValueResolver value) {

        final String id = (String) value.value(Name.of(ReferableSchema.ID));
        if (id != null) {
            final Map<String, Object> result = new HashMap<>();
            result.put(ReferableSchema.ID, id);
            if (type.isVersioned()) {
                result.put(ReferableSchema.VERSION, UseInteger.DEFAULT.create(value.value(Name.of(ReferableSchema.VERSION))));
            }
            return new Instance(result);
        } else {
            return null;
        }
    }

    @Override
    public Bytes binaryFromSQLValue(final UseBinary type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            return Bytes.fromBase64((String) v);
        }
    }

    @Override
    public Object anyFromSQLValue(final UseAny type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            return fromJson(v, new TypeReference<Object>() {
            });
        }
    }

    @Override
    public <V, T extends Collection<V>> List<Field<?>> selectCollection(final UseCollection<V, T> type, final Name name, final FieldResolver field) {

        return field.field().<List<Field<?>>>map(f -> ImmutableList.of(f.cast(jsonType()).as(columnName(name)))).orElseGet(ImmutableList::of);
    }

    @Override
    public List<Field<?>> selectRef(final UseRef type, final Name name, final FieldResolver field) {

        final List<Field<?>> fields = new ArrayList<>(selectFields(field.resolver(Name.of(ReferableSchema.ID)), name.with(ReferableSchema.ID), UseString.DEFAULT));
        if (type.isVersioned()) {
            fields.addAll(selectFields(field.resolver(Name.of(ReferableSchema.VERSION)), name.with(ReferableSchema.VERSION), UseInteger.DEFAULT));
        }
        return fields;
    }

    @Override
    public <V> List<Field<?>> selectMap(final UseMap<V> type, final Name name, final FieldResolver field) {

        return field.field().<List<Field<?>>>map(f -> ImmutableList.of(f.cast(jsonType()).as(columnName(name)))).orElseGet(ImmutableList::of);
    }

    @Override
    public List<Field<?>> selectStruct(final UseStruct type, final Name name, final FieldResolver field) {

        final List<Field<?>> fields = new ArrayList<>();
        for (final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
            fields.addAll(selectFields(field.resolver(Name.of(entry.getKey())), name.with(entry.getKey()), entry.getValue().typeOf()));
        }
        return fields;
    }

    @Override
    public List<Field<?>> selectView(final UseView type, final Name name, final FieldResolver field) {

        return field.field().<List<Field<?>>>map(f -> ImmutableList.of(f.cast(jsonType()).as(columnName(name)))).orElseGet(ImmutableList::of);
    }

    protected SelectField<?> toJson(final Object value) {

        if (value == null) {
            return null;
        }
        try {
            return castJson(objectMapper.writeValueAsString(value));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected SelectField<?> castJson(final String value) {

        return DSL.val(JSON.valueOf(value));
    }

    protected <T> T fromJson(final Object value, final TypeReference<T> ref) {

        if (value == null) {
            return null;
        }
        final String str;
        if (value instanceof JSON) {
            str = unescapeJson(((JSON) value).data());
        } else if (value instanceof JSONB) {
            str = unescapeJson(((JSONB) value).data());
        } else if(value instanceof String) {
            str = (String)value;
        } else {
            log.error("Unexpected JSON type {} ({})", value.getClass(), value);
            return null;
        }
        try {
            return objectMapper.readValue(str, ref);
        } catch (final IOException e) {
            log.error("Failed to read JSON ({})", str);
            return null;
        }
    }

    protected String unescapeJson(final String data) {

        if(isJsonEscaped()) {
            return StringEscapeUtils.unescapeJava(data.substring(1, data.length() - 1));
        } else {
            return data;
        }
    }
}
