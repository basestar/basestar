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

import io.basestar.schema.*;
import io.basestar.schema.use.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.*;
import java.util.stream.Collectors;

public class AvroUtils {

    private static String name(final io.basestar.schema.Schema<?> schema) {

        return schema.getQualifiedName().toString(Reserved.PREFIX);
    }

    public static Schema schema(final io.basestar.schema.Schema<?> schema) {

        if(schema instanceof EnumSchema) {
            final EnumSchema enumSchema = (EnumSchema)schema;
            final List<String> values = enumSchema.getValues();
            return Schema.createEnum(name(schema), schema.getDescription(), null, values);
        } else if(schema instanceof InstanceSchema) {
            final InstanceSchema instanceSchema = (InstanceSchema)schema;
            final List<Schema.Field> fields = new ArrayList<>();
            instanceSchema.metadataSchema()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v))));
            instanceSchema.getProperties()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v))));
            return Schema.createRecord(name(schema), schema.getDescription(), null, false, fields);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Schema refSchema(final ObjectSchema schema) {

        final List<Schema.Field> fields = new ArrayList<>();
        ObjectSchema.REF_SCHEMA
                .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v))));
        return Schema.createRecord(name(schema), schema.getDescription(), null, false, fields);
    }

    public static Schema schema(final Property property) {

        return schema(property.getType());
    }

    public static Schema schema(final Use<?> use) {

        return use.visit(new Use.Visitor<Schema>() {

            @Override
            public Schema visitBoolean(final UseBoolean type) {

                return Schema.create(Schema.Type.BOOLEAN);
            }

            @Override
            public Schema visitInteger(final UseInteger type) {

                return Schema.create(Schema.Type.LONG);
            }

            @Override
            public Schema visitNumber(final UseNumber type) {

                return Schema.create(Schema.Type.DOUBLE);
            }

            @Override
            public Schema visitString(final UseString type) {

                return Schema.create(Schema.Type.STRING);
            }

            @Override
            public Schema visitEnum(final UseEnum type) {

                return schema(type.getSchema());
            }

            @Override
            public Schema visitObject(final UseObject type) {

                return refSchema(type.getSchema());
            }

            @Override
            public <T> Schema visitArray(final UseArray<T> type) {

                return Schema.createArray(schema(type.getType()));
            }

            @Override
            public <T> Schema visitSet(final UseSet<T> type) {

                return Schema.createArray(schema(type.getType()));
            }

            @Override
            public <T> Schema visitMap(final UseMap<T> type) {

                return Schema.createMap(schema(type.getType()));
            }

            @Override
            public Schema visitStruct(final UseStruct type) {

                return schema(type.getSchema());
            }

            @Override
            public Schema visitBinary(final UseBinary type) {

                return Schema.create(Schema.Type.BYTES);
            }

            @Override
            public Schema visitDate(final UseDate type) {

                return Schema.create(Schema.Type.STRING);
            }

            @Override
            public Schema visitDateTime(final UseDateTime type) {

                return Schema.create(Schema.Type.STRING);
            }

            @Override
            public Schema visitView(final UseView type) {

                return schema(type.getSchema());
            }

            @Override
            public <T> Schema visitOptional(final UseOptional<T> type) {

                return Schema.createUnion(schema(type.getType()), Schema.create(Schema.Type.NULL));
            }
        });
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Schema schema, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), object.get(k)));
        });
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v.getType(), field.schema(), object.get(k)));
        });
        return record;
    }

    public static GenericRecord encodeRef(final Schema schema, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        ObjectSchema.REF_SCHEMA.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), object.get(k)));
        });
        return record;
    }

    @SuppressWarnings("unchecked")
    private static Object encode(final Use<?> use, final Schema schema, final Object value) {

        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public GenericRecord visitObject(final UseObject type) {

                return value == null ? null : encodeRef(schema, (Map<String, Object>)value);
            }

            @Override
            public GenericRecord visitInstance(final UseInstance type) {

                return value == null ? null : encode(type.getSchema(), schema, (Map<String, Object>)value);
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                if(value == null) {
                    return null;
                } else {
                    final Collection<?> arr = (Collection<?>)value;
                    return arr.stream()
                            .map(v -> encode(type.getType(), schema.getElementType(), v))
                            .collect(Collectors.toList());
                }
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                if(value == null) {
                    return null;
                } else {
                    final Map<?, ?> map = (Map<?, ?>)value;
                    return map.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> encode(type.getType(), schema.getValueType(), e.getValue())
                            ));
                }
            }

            @Override
            public String visitDate(final UseDate type) {

                return value == null ? null : value.toString();
            }

            @Override
            public String visitDateTime(final UseDateTime type) {

                return value == null ? null : value.toString();
            }
        });
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Schema schema, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v, field.schema(), record.get(field.pos())));
        });
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v.getType(), field.schema(), record.get(field.pos())));
        });
        return object;
    }

    public static Map<String, Object> decodeRef(final Schema schema, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        ObjectSchema.REF_SCHEMA.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, encode(v, field.schema(), record.get(field.pos())));
        });
        return object;
    }

    private static Object decode(final Use<?> use, final Schema schema, final Object value) {

        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public Map<String, Object> visitObject(final UseObject type) {

                return value == null ? null : decodeRef(schema, (IndexedRecord)value);
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                if(value == null) {
                    return null;
                } else {
                    final Collection<?> arr = (Collection<?>)value;
                    return arr.stream()
                            .map(v -> decode(type.getType(), schema.getElementType(), v))
                            .collect(Collectors.toList());
                }
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                if(value == null) {
                    return null;
                } else {
                    final Map<?, ?> map = (Map<?, ?>)value;
                    return map.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> decode(type.getType(), schema.getValueType(), e.getValue())
                            ));
                }
            }

            @Override
            public Map<String, Object> visitInstance(final UseInstance type) {

                return value == null ? null : decode(type.getSchema(), schema, (IndexedRecord)value);
            }
        });
    }
}
