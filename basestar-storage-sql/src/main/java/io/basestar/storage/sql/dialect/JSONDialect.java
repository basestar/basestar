package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Instance;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.use.*;
import io.basestar.storage.sql.SQLDialect;
import io.basestar.util.Bytes;
import io.basestar.util.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.JSONB;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public <T> Object arrayToSQLValue(final UseArray<T> type, final List<T> value) {

        return toJson(value);
    }

    @Override
    public <T> Object pageToSQLValue(final UsePage<T> type, final Page<T> value) {

        return toJson(value);
    }

    @Override
    public <T> Object setToSQLValue(final UseSet<T> type, final Set<T> value) {

        return toJson(value);
    }

    @Override
    public <T> Object mapToSQLValue(final UseMap<T> type, final Map<String, T> value) {

        return toJson(value);
    }

    @Override
    public Object structToSQLValue(final UseStruct type, final Instance value) {

        return toJson(value);
    }

    @Override
    public Object viewToSQLValue(final UseView type, final Instance value) {

        return toJson(value);
    }

    @Override
    public Object refToSQLValue(final UseRef type, final Instance value) {

        return Instance.getId(value);
    }

    @Override
    public Object binaryToSQLValue(final UseBinary type, final Bytes value) {

        return value.toBase64();
    }

    @Override
    public Object anyToSQLValue(final UseAny type, final Object value) {

        return toJson(value);
    }

    @Override
    public <T> List<T> arrayFromSQLValue(final UseArray<T> type, final Object value) {

        final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
        return type.create(results);
    }

    @Override
    public <T> Page<T> pageFromSQLValue(final UsePage<T> type, final Object value) {

        final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
        return type.create(results);
    }

    @Override
    public <T> Set<T> setFromSQLValue(final UseSet<T> type, final Object value) {

        final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
        return type.create(results);
    }

    @Override
    public <T> Map<String, T> mapFromSQLValue(final UseMap<T> type, final Object value) {

        return type.create(fromJson(value, new TypeReference<Map<String, ?>>() {}));
    }

    @Override
    public Instance structFromSQLValue(final UseStruct type, final Object value) {

        return type.create(fromJson(value, new TypeReference<Map<String, Object>>() {}));
    }

    @Override
    public Instance viewFromSQLValue(final UseView type, final Object value) {

        return type.create(fromJson(value, new TypeReference<Map<String, Object>>() {}));
    }

    @Override
    public Instance refFromSQLValue(final UseRef type, final Object value) {

        final String id = (String)value;
        return ReferableSchema.ref(id);
    }

    @Override
    public Bytes binaryFromSQLValue(final UseBinary type, final Object value) {

        return Bytes.fromBase64((String)value);
    }

    @Override
    public Object anyFromSQLValue(final UseAny type, final Object value) {

        return fromJson(value, new TypeReference<Object>() {});
    }

    @Override
    public <V, T extends Collection<V>> Field<?> selectCollection(final UseCollection<V, T> type, final Field<?> field) {

        return field.cast(jsonType());
    }

    @Override
    public <V> Field<?> selectMap(final UseMap<V> type, final Field<?> field) {

        return field.cast(jsonType());
    }

    @Override
    public Field<?> selectStruct(final UseStruct type, final Field<?> field) {

        return field.cast(jsonType());
    }

    protected JSON toJson(final Object value) {

        if(value == null) {
            return null;
        }
        try {
            return JSON.valueOf(objectMapper.writeValueAsString(value));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected <T> T fromJson(final Object value, final TypeReference<T> ref) {

        if(value == null) {
            return null;
        }
        try {
            final String str;
            if(value instanceof JSON) {
                str = unescapeJson(((JSON)value).data());
            } else if(value instanceof JSONB) {
                str = unescapeJson(((JSONB)value).data());
            } else {
                log.error("Unexpected JSON type {} ({})", value.getClass(), value);
                return null;
            }
            return objectMapper.readValue(str, ref);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
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
