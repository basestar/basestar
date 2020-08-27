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
import io.basestar.util.Name;
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

        if(schema instanceof InstanceSchema) {
            return schema(schema, ((InstanceSchema) schema).getExpand());
        } else {
            return schema(schema, Collections.emptySet());
        }
    }

    public static Schema schema(final io.basestar.schema.Schema<?> schema, final Set<Name> expand) {

        if(schema instanceof EnumSchema) {
            final EnumSchema enumSchema = (EnumSchema)schema;
            final List<String> values = enumSchema.getValues();
            return Schema.createEnum(name(schema), schema.getDescription(), null, values);
        } else if(schema instanceof InstanceSchema) {
            final InstanceSchema instanceSchema = (InstanceSchema)schema;
            final List<Schema.Field> fields = new ArrayList<>();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            instanceSchema.metadataSchema()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, Collections.emptySet()))));
            instanceSchema.getProperties()
                    .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, branches.get(k)))));
            return Schema.createRecord(name(schema), schema.getDescription(), null, false, fields);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Schema refSchema(final ObjectSchema schema) {

        final List<Schema.Field> fields = new ArrayList<>();
        ObjectSchema.REF_SCHEMA
                .forEach((k, v) -> fields.add(new Schema.Field(k, schema(v, Collections.emptySet()))));
        return Schema.createRecord(name(schema), schema.getDescription(), null, false, fields);
    }

    public static Schema schema(final Property property, final Set<Name> expand) {

        return schema(property.getType(), expand);
    }

    public static Schema schema(final Use<?> use, final Set<Name> expand) {

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

                return schema(type.getSchema(), expand);
            }

            @Override
            public Schema visitObject(final UseObject type) {

                if(expand == null) {
                    return refSchema(type.getSchema());
                } else {
                    return schema(type.getSchema(), expand);
                }
            }

            @Override
            public <T> Schema visitArray(final UseArray<T> type) {

                return Schema.createArray(schema(type.getType(), expand));
            }

            @Override
            public <T> Schema visitSet(final UseSet<T> type) {

                return Schema.createArray(schema(type.getType(), expand));
            }

            @Override
            public <T> Schema visitMap(final UseMap<T> type) {

                return Schema.createMap(schema(type.getType(), expand));
            }

            @Override
            public Schema visitStruct(final UseStruct type) {

                return schema(type.getSchema(), expand);
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

                return schema(type.getSchema(), expand);
            }

            @Override
            public <T> Schema visitOptional(final UseOptional<T> type) {

                return Schema.createUnion(schema(type.getType(), expand), Schema.create(Schema.Type.NULL));
            }
        });
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Schema schema, final Map<String, Object> object) {

        return encode(instanceSchema, schema, instanceSchema.getExpand(), object);
    }

    public static GenericRecord encode(final InstanceSchema instanceSchema, final Schema schema, final Set<Name> expand, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), Collections.emptySet(), object.get(k)));
        });
        final Map<String, Set<Name>> branches = Name.branch(expand);
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v.getType(), field.schema(), branches.get(k), object.get(k)));
        });
        return record;
    }

    public static GenericRecord encodeRef(final Schema schema, final Map<String, Object> object) {

        final GenericRecord record = new GenericData.Record(schema);
        ObjectSchema.REF_SCHEMA.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            record.put(k, encode(v, field.schema(), Collections.emptySet(), object.get(k)));
        });
        return record;
    }

    @SuppressWarnings("unchecked")
    private static Object encode(final Use<?> use, final Schema schema, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public GenericRecord visitObject(final UseObject type) {

                if(expand == null) {
                    return encodeRef(schema, (Map<String, Object>) value);
                } else {
                    return encode(type.getSchema(), schema, expand, (Map<String, Object>)value);
                }
            }

            @Override
            public GenericRecord visitInstance(final UseInstance type) {

                return encode(type.getSchema(), schema, expand, (Map<String, Object>)value);
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                final Collection<?> arr = (Collection<?>)value;
                return arr.stream()
                        .map(v -> encode(type.getType(), schema.getElementType(), expand, v))
                        .collect(Collectors.toList());
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                final Map<?, ?> map = (Map<?, ?>)value;
                return map.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> encode(type.getType(), schema.getValueType(), expand, e.getValue())
                        ));
            }

            @Override
            public <T> String visitStringLike(final UseStringLike<T> type) {

                return type.toString(type.create(value));
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return encode(type.getType(), unwrapOptionalSchema(schema), expand, value);
            }
        });
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Schema schema, final IndexedRecord record) {

        return decode(instanceSchema, schema, instanceSchema.getExpand(), record);
    }

    public static Map<String, Object> decode(final InstanceSchema instanceSchema, final Schema schema, final Set<Name> expand, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        instanceSchema.metadataSchema().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v, field.schema(), Collections.emptySet(), record.get(field.pos())));
        });
        final Map<String, Set<Name>> branches = Name.branch(expand);
        instanceSchema.getProperties().forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, decode(v.getType(), field.schema(), branches.get(k), record.get(field.pos())));
        });
        return object;
    }

    public static Map<String, Object> decodeRef(final Schema schema, final IndexedRecord record) {

        final Map<String, Object> object = new HashMap<>();
        ObjectSchema.REF_SCHEMA.forEach((k, v) -> {
            final Schema.Field field = schema.getField(k);
            object.put(k, encode(v, field.schema(), Collections.emptySet(), record.get(field.pos())));
        });
        return object;
    }

    private static Object decode(final Use<?> use, final Schema schema, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return use.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public Map<String, Object> visitObject(final UseObject type) {

                if(expand == null) {
                    return decodeRef(schema, (IndexedRecord) value);
                } else {
                    return decode(type.getSchema(), schema, expand, (IndexedRecord)value);
                }
            }

            @Override
            public <V, T extends Collection<V>> List<?> visitCollection(final UseCollection<V, T> type) {

                final Collection<?> arr = (Collection<?>)value;
                return arr.stream()
                        .map(v -> decode(type.getType(), schema.getElementType(), expand, v))
                        .collect(Collectors.toList());
            }

            @Override
            public <T> Map<?, ?> visitMap(final UseMap<T> type) {

                final Map<?, ?> map = (Map<?, ?>)value;
                return map.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> decode(type.getType(), schema.getValueType(), expand, e.getValue())
                        ));
            }

            @Override
            public Map<String, Object> visitInstance(final UseInstance type) {

                return decode(type.getSchema(), schema, expand, (IndexedRecord)value);
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return decode(type.getType(), unwrapOptionalSchema(schema), expand, value);
            }
        });
    }

    private static Schema unwrapOptionalSchema(final Schema schema) {

        if(schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream().filter(v -> v.getType() != Schema.Type.NULL).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Invalid Avro schema"));
        } else {
            return schema;
        }
    }
}
