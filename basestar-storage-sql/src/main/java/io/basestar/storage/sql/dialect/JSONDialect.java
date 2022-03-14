package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.StructSchema;
import io.basestar.schema.use.*;
import io.basestar.secret.Secret;
import io.basestar.storage.sql.SQLDialect;
import io.basestar.storage.sql.mapping.PropertyMapping;
import io.basestar.storage.sql.mapping.ValueTransform;
import io.basestar.util.Bytes;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;
import org.jooq.DataType;
import org.jooq.JSON;
import org.jooq.JSONB;
import org.jooq.SelectField;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class JSONDialect implements SQLDialect {

    protected static final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    protected abstract boolean isJsonEscaped();

    protected DataType<?> jsonType() {

        return SQLDataType.JSON;
    }

    @Override
    public DataType<String> stringType(final UseStringLike<?> type) {

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

        if (isJsonEscaped()) {
            return StringEscapeUtils.unescapeJava(data.substring(1, data.length() - 1));
        } else {
            return data;
        }
    }

    public <T> PropertyMapping<T> jsonMapping(final Use<T> type, final Set<Name> expand) {

        return PropertyMapping.simple(SQLDataType.JSON, new ValueTransform<T, JSON>() {
            @Override
            public SelectField<JSON> toSQLValue(final T value) {

                try {
                    return DSL.inline(value == null ? null : JSON.valueOf(objectMapper.writeValueAsString(value)));
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public T fromSQLValue(final JSON value, final Set<Name> expand) {

                try {
                    if (value == null) {
                        return null;
                    } else {
                        final String data = unescapeJson(value.data());
                        return type.create(objectMapper.readValue(data, Object.class), expand);
                    }
                } catch (final IOException e) {
                    throw new IllegalStateException();
                }
            }
        });
    }

    @Override
    public PropertyMapping<LocalDate> dateMapping(final UseDate type) {

        return PropertyMapping.simple(dateType(type), new ValueTransform<LocalDate, java.sql.Date>() {
            @Override
            public SelectField<java.sql.Date> toSQLValue(final LocalDate value) {

                return DSL.inline(value == null ? null : ISO8601.toSqlDate(value));
            }

            @Override
            public LocalDate fromSQLValue(final java.sql.Date value, final Set<Name> expand) {

                return value == null ? null : ISO8601.toDate(value);
            }
        });
    }

    @Override
    public PropertyMapping<Instant> dateTimeMapping(final UseDateTime type) {

        return PropertyMapping.simple(dateTimeType(type), new ValueTransform<Instant, java.sql.Timestamp>() {
            @Override
            public SelectField<java.sql.Timestamp> toSQLValue(final Instant value) {

                return DSL.inline(value == null ? null : ISO8601.toSqlTimestamp(value));
            }

            @Override
            public Instant fromSQLValue(final java.sql.Timestamp value, final Set<Name> expand) {

                return value == null ? null : ISO8601.toDateTime(value);
            }
        });
    }

    @Override
    public PropertyMapping<Map<String, Object>> refMapping(final UseRef type, final Set<Name> expand) {

        final Map<String, PropertyMapping<?>> mappings = new HashMap<>();
        mappings.put(ReferableSchema.ID, stringMapping(UseString.DEFAULT));
        if (type.isVersioned()) {
            mappings.put(ReferableSchema.VERSION, integerMapping(UseInteger.DEFAULT));
        }
        final PropertyMapping<Map<String, Object>> mapping = PropertyMapping.flattenedRef(mappings);
        if (expand == null) {
            return mapping;
        } else {
            return PropertyMapping.expandedRef(mapping, schemaMapping(type.getSchema(), type.isVersioned(), expand));
        }
    }

    @Override
    public PropertyMapping<Map<String, Object>> structMapping(final UseStruct type, final Set<Name> expand) {

        final StructSchema structSchema = type.getSchema();
        return PropertyMapping.flattenedStruct(flattenedMappings(structSchema, expand));
    }

    protected Map<String, PropertyMapping<?>> flattenedMappings(final InstanceSchema schema, final Set<Name> expand) {

        final Map<String, PropertyMapping<?>> mappings = new HashMap<>();
        final Map<String, Set<Name>> branches = Name.branch(expand);
        schema.layoutSchema(expand).forEach((propName, propType) -> {
            final Set<Name> branch = branches.get(propName);
            mappings.put(propName, propertyMapping(propType, branch));
        });
        return mappings;
    }

    @Override
    public PropertyMapping<Map<String, Object>> viewMapping(final UseView type, final Set<Name> expand) {

        return jsonMapping(UseMap.DEFAULT, expand);
    }

    @Override
    public <T> PropertyMapping<List<T>> arrayMapping(final UseArray<T> type, final Set<Name> expand) {

        return jsonMapping(type, expand);
    }

    @Override
    public <T> PropertyMapping<Page<T>> pageMapping(final UsePage<T> type, final Set<Name> expand) {

        return jsonMapping(type, expand);
    }

    @Override
    public <T> PropertyMapping<Set<T>> setMapping(final UseSet<T> type, final Set<Name> expand) {

        return jsonMapping(type, expand);
    }

    @Override
    public <T> PropertyMapping<Map<String, T>> mapMapping(final UseMap<T> type, final Set<Name> expand) {

        return jsonMapping(type, expand);
    }

    @Override
    public PropertyMapping<Bytes> binaryMapping(final UseBinary type) {

        return PropertyMapping.simple(stringType(UseString.DEFAULT), new ValueTransform<Bytes, String>() {

            @Override
            public SelectField<String> toSQLValue(final Bytes value) {

                return DSL.inline(value == null ? null : value.toBase64());
            }

            @Override
            public Bytes fromSQLValue(final String value, final Set<Name> expand) {

                return value == null ? null : Bytes.fromBase64(value);
            }
        });
    }

    @Override
    public PropertyMapping<Secret> secretMapping(final UseSecret type) {

        return PropertyMapping.simple(stringType(UseString.DEFAULT), new ValueTransform<Secret, String>() {

            @Override
            public SelectField<String> toSQLValue(final Secret value) {

                return DSL.inline(value == null ? null : value.encryptedBase64());
            }

            @Override
            public Secret fromSQLValue(final String value, final Set<Name> expand) {

                return value == null ? null : Secret.encrypted(value);
            }
        });
    }

    @Override
    public <T> PropertyMapping<Object> anyMapping(final UseAny type) {

        return jsonMapping(type, null);
    }
}
