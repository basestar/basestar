package io.basestar.avro;

/*-
 * #%L
 * basestar-avro
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.type.Coercion;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class AvroUtils {

    private AvroUtils() {

    }

    private static String name(final io.basestar.schema.Schema schema) {

        return schema.getQualifiedName().toString(Reserved.PREFIX);
    }

    public static Schema schema(final io.basestar.schema.Schema schema) {

        if (schema instanceof InstanceSchema) {
            return schema(schema, ((InstanceSchema) schema).getExpand());
        } else {
            return schema(schema, Collections.emptySet());
        }
    }

    public static Schema schema(final io.basestar.schema.Schema schema, final Map<String, Use<?>> additionalMetadata) {

        if (schema instanceof InstanceSchema) {
            return schema(schema, additionalMetadata, ((InstanceSchema) schema).getExpand());
        } else {
            return schema(schema, additionalMetadata, ImmutableSet.of());
        }
    }

    public static Schema schema(final io.basestar.schema.Schema schema, final Set<Name> expand) {

        return schema(schema, ImmutableMap.of(), expand);
    }

    public static Schema schema(final io.basestar.schema.Schema schema, final Map<String, Use<?>> additionalMetadata, final Set<Name> expand) {

        if (schema instanceof EnumSchema) {
            final EnumSchema enumSchema = (EnumSchema) schema;
            final List<String> values = enumSchema.getValues();
            return Schema.createEnum(name(schema), schema.getDescription(), null, values);
        } else if (schema instanceof InstanceSchema) {
            final InstanceSchema instanceSchema = (InstanceSchema) schema;
            final List<Schema.Field> fields = new ArrayList<>();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            instanceSchema.metadataSchema()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, Collections.emptySet(), false))));
            instanceSchema.getProperties()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, branches.get(k)))));
            additionalMetadata.forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, branches.get(k), false))));
            return Schema.createRecord(name(schema), schema.getDescription(), null, false, fields);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Schema refSchema(final boolean versioned) {

        final List<Schema.Field> fields = new ArrayList<>();
        ReferableSchema.refSchema(versioned).forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, Collections.emptySet(), false))));
        return Schema.createRecord(Reserved.PREFIX + "Ref", "Ref", null, false, fields);
    }

    public static Schema schema(final Property property, final Set<Name> expand) {

        return schema(property.typeOf(), expand, false);
    }

    public static Schema schema(final Use<?> use, final Set<Name> expand, final boolean optional) {

        return use.visit(new Use.Visitor<Schema>() {

            @Override
            public Schema visitBoolean(final UseBoolean type) {

                return makeOptional(Schema.create(Schema.Type.BOOLEAN), optional);
            }

            @Override
            public Schema visitInteger(final UseInteger type) {

                return makeOptional(Schema.create(Schema.Type.LONG), optional);
            }

            @Override
            public Schema visitNumber(final UseNumber type) {

                return makeOptional(Schema.create(Schema.Type.DOUBLE), optional);
            }

            @Override
            public Schema visitString(final UseString type) {

                return makeOptional(Schema.create(Schema.Type.STRING), optional);
            }

            @Override
            public Schema visitEnum(final UseEnum type) {

                return makeOptional(schema(type.getSchema(), expand), optional);
            }

            @Override
            public Schema visitRef(final UseRef type) {

                if (expand == null) {
                    return makeOptional(refSchema(type.isVersioned()), true);
                } else {
                    return makeOptional(schema(type.getSchema(), expand), true);
                }
            }

            @Override
            public <T> Schema visitArray(final UseArray<T> type) {

                return makeOptional(Schema.createArray(schema(type.getType(), expand, false)), optional);
            }

            @Override
            public <T> Schema visitPage(final UsePage<T> type) {

                return makeOptional(Schema.createArray(schema(type.getType(), expand, false)), optional);
            }

            @Override
            public Schema visitDecimal(final UseDecimal useDecimal) {

                return makeOptional(Schema.create(Schema.Type.STRING), optional);
            }

            @Override
            public Schema visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Schema visitSet(final UseSet<T> type) {

                return makeOptional(Schema.createArray(schema(type.getType(), expand, false)), optional);
            }

            @Override
            public <T> Schema visitMap(final UseMap<T> type) {

                return makeOptional(Schema.createMap(schema(type.getType(), expand, false)), optional);
            }

            @Override
            public Schema visitStruct(final UseStruct type) {

                return makeOptional(schema(type.getSchema(), expand), optional);
            }

            @Override
            public Schema visitBinary(final UseBinary type) {

                return makeOptional(Schema.create(Schema.Type.BYTES), optional);
            }

            @Override
            public Schema visitDate(final UseDate type) {

                return makeOptional(Schema.create(Schema.Type.STRING), optional);
            }

            @Override
            public Schema visitDateTime(final UseDateTime type) {

                return makeOptional(Schema.create(Schema.Type.STRING), optional);
            }

            @Override
            public Schema visitView(final UseView type) {

                return makeOptional(schema(type.getSchema(), expand), true);
            }

            @Override
            public Schema visitQuery(final UseQuery type) {

                return makeOptional(schema(type.getSchema(), expand), true);
            }

            @Override
            public <T> Schema visitOptional(final UseOptional<T> type) {

                return schema(type.getType(), expand, true);
            }

            @Override
            public Schema visitAny(final UseAny type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public Schema visitSecret(final UseSecret type) {

                return makeOptional(Schema.create(Schema.Type.BYTES), true);
            }
        });
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Schema schema, final Map<String, Object> object) {

        return encode(instanceSchema, Immutable.map(), schema, instanceSchema.getExpand(), object);
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Map<String, Use<?>> additionalMetadata, final Schema schema, final Map<String, Object> object) {

        return encode(instanceSchema, additionalMetadata, schema, instanceSchema.getExpand(), object);
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Schema schema, final Set<Name> expand, final Map<String, Object> object) {

        return encode(instanceSchema, Immutable.map(), schema, expand, object);
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Map<String, Use<?>> additionalMetadata, final Schema schema, final Set<Name> expand, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), Collections.emptySet(), object.get(k), false));
        });
        final Map<String, Set<Name>> branches = Name.branch(expand);
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v.typeOf(), field.schema(), branches.get(k), object.get(k), false));
        });
        additionalMetadata.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), branches.get(k), object.get(k), false));
        });
        return record;
    }

    public static GenericRecord encodeRef(final Schema schema, final boolean versioned, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        ReferableSchema.refSchema(versioned).forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), Collections.emptySet(), object.get(k), false));
        });
        return record;
    }

    @SuppressWarnings("unchecked")
    public static Object encode(final Use<?> use, final Schema schema, final Set<Name> expand, final Object value, final boolean optional) {

        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                if (value == null) {
                    return optional ? null : use.defaultValue();
                } else {
                    return type.create(value);
                }
            }

            @Override
            public GenericRecord visitRef(final UseRef type) {

                if (value == null) {
                    return null;
                } else if (expand == null) {
                    return encodeRef(unwrapOptionalSchema(schema), type.isVersioned(), (Map<String, Object>) value);
                } else {
                    return encode(type.getSchema(), unwrapOptionalSchema(schema), expand, (Map<String, Object>) value);
                }
            }

            @Override
            public GenericRecord visitInstance(final UseInstance type) {

                if (value == null) {
                    return null;
                } else {
                    return encode(type.getSchema(), unwrapOptionalSchema(schema), expand, (Map<String, Object>) value);
                }
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                if (value == null) {
                    return optional ? null : Collections.emptyList();
                } else {
                    final Collection<?> arr = (Collection<?>) value;
                    return arr.stream()
                            .map(v -> encode(type.getType(), unwrapOptionalSchema(schema.getElementType()), expand, v, false))
                            .collect(Collectors.toList());
                }
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                if (value == null) {
                    return optional ? null : Collections.emptyMap();
                } else {
                    final Map<?, ?> map = (Map<?, ?>) value;
                    final Map<String, Object> result = new HashMap<>();
                    map.forEach((k, v) -> result.put(Coercion.toString(k), encode(type.getType(), unwrapOptionalSchema(schema.getValueType()), expand, v, false)));
                    return result;
                }
            }

            @Override
            public <T> String visitStringLike(final UseStringLike<T> type) {

                if (value == null) {
                    return optional ? null : type.toString(type.defaultValue());
                } else {
                    return type.toString(type.create(value));
                }
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return encode(type.getType(), unwrapOptionalSchema(schema), expand, value, true);
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                if (value == null) {
                    return optional ? null : ByteBuffer.wrap(new byte[0]);
                } else {
                    return Nullsafe.map(type.create(value), v -> ByteBuffer.wrap(v.getBytes()));
                }
            }
        });
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Schema schema, final IndexedRecord record) {

        return decode(instanceSchema, schema, instanceSchema.getExpand(), record);
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Schema schema, final Set<Name> expand, final IndexedRecord record) {

        return decode(instanceSchema, Immutable.map(), schema, expand, record);
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Map<String, Use<?>> additionalMetadata, final Schema schema, final IndexedRecord record) {

        return decode(instanceSchema, additionalMetadata, schema, ImmutableSet.of(), record);
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Map<String, Use<?>> additionalMetadata, final Schema schema, final Set<Name> expand, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v, field.schema(), Collections.emptySet(), record.get(field.pos())));
        });
        final Map<String, Set<Name>> branches = Name.branch(expand);
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v.typeOf(), field.schema(), branches.get(k), record.get(field.pos())));
        });
        additionalMetadata.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v, field.schema(), branches.get(k), record.get(field.pos())));
        });
        return object;
    }

    public static Map<String, Object> decodeRef(final Schema schema, final boolean versioned, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        ReferableSchema.refSchema(versioned).forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, encode(v, field.schema(), Collections.emptySet(), record.get(field.pos()), false));
        });
        return object;
    }

    public static Object decode(final Use<?> use, final Schema schema, final Set<Name> expand, final Object value) {

        if (value == null) {
            return null;
        }
        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public Map<String, Object> visitRef(final UseRef type) {

                if (expand == null) {
                    return decodeRef(unwrapOptionalSchema(schema), type.isVersioned(), (IndexedRecord) value);
                } else {
                    return decode(type.getSchema(), unwrapOptionalSchema(schema), expand, (IndexedRecord) value);
                }
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                final Collection<?> arr = (Collection<?>) value;
                return arr.stream()
                        .map(v -> decode(type.getType(), unwrapOptionalSchema(schema.getElementType()), expand, v))
                        .collect(Collectors.toList());
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                final Map<?, ?> map = (Map<?, ?>) value;
                return map.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> decode(type.getType(), unwrapOptionalSchema(schema.getValueType()), expand, e.getValue())
                        ));
            }

            @Override
            public Map<String, Object> visitInstance(final UseInstance type) {

                return decode(type.getSchema(), unwrapOptionalSchema(schema), expand, (IndexedRecord) value);
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return decode(type.getType(), unwrapOptionalSchema(schema), expand, value);
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return type.create(value);
            }
        });
    }

    private static Schema unwrapOptionalSchema(final Schema schema) {

        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream().filter(v -> v.getType() != Schema.Type.NULL).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Invalid Avro schema"));
        } else {
            return schema;
        }
    }

    private static Schema makeOptional(final Schema schema, final boolean optional) {

        if (optional) {
            return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
        } else {
            return schema;
        }
    }
}
