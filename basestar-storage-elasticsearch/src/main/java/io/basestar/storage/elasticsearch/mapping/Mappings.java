package io.basestar.storage.elasticsearch.mapping;

/*-
 * #%L
 * basestar-storage-elasticsearch
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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class Mappings {

    private final Map<String, FieldType> properties;

    public Map<String, ?> source() {

        return ImmutableMap.of("properties", properties.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().source())));
    }

    public interface Factory {

        Mappings mappings(ReferableSchema schema);

        class Default implements Factory {

            @Override
            public Mappings mappings(final ReferableSchema schema) {

                return new Mappings(properties(schema, schema.getExpand()));
            }

            protected Map<String, FieldType> properties(final InstanceSchema schema, final Set<Name> expand) {

                final Map<String, FieldType> properties = new HashMap<>();
                final Map<String, Set<Name>> branches = Name.branch(expand);
                schema.metadataSchema().forEach((k, v) -> properties.put(k, fieldType(schema, k, v, branches.get(k))));
                schema.getProperties().forEach((k, v) -> properties.put(k, fieldType(schema, k, v.getType(), branches.get(k))));
                return properties;
            }

            protected FieldType fieldType(final InstanceSchema schema, final String name, final Use<?> use, final Set<Name> expand) {

                if(schema instanceof ObjectSchema) {
                    switch (name) {
                        case ObjectSchema.ID:
                        case ObjectSchema.SCHEMA:
                            return FieldType.KEYWORD;
                        case ObjectSchema.CREATED:
                        case ObjectSchema.UPDATED:
                            return FieldType.DATETIME;
                        default:
                            return fieldType(use, expand);
                    }
                } else {
                    return fieldType(use, expand);
                }
            }

            protected FieldType fieldType(final Use<?> use, final Set<Name> expand) {

                return use.visit(new Use.Visitor<FieldType>() {

                    @Override
                    public FieldType visitBoolean(final UseBoolean type) {

                        return FieldType.BOOLEAN;
                    }

                    @Override
                    public FieldType visitInteger(final UseInteger type) {

                        return FieldType.LONG;
                    }

                    @Override
                    public FieldType visitNumber(final UseNumber type) {

                        return FieldType.DOUBLE;
                    }

                    @Override
                    public FieldType visitString(final UseString type) {

                        return FieldType.TEXT;
                    }

                    @Override
                    public FieldType visitEnum(final UseEnum type) {

                        return FieldType.KEYWORD;
                    }

                    @Override
                    public FieldType visitRef(final UseRef type) {

                        if(expand == null) {
                            final Map<String, FieldType> properties = new HashMap<>();
                            ReferableSchema.refSchema(type.isVersioned()).forEach((k, v) -> properties.put(k, v.visit(this)));
                            return new FieldType.NestedType(properties);
                        } else {
                            return new FieldType.NestedType(properties(type.getSchema(), expand));
                        }
                    }

                    @Override
                    public <T> FieldType visitArray(final UseArray<T> type) {

                        return new FieldType.ArrayType(type.getType().visit(this));
                    }

                    @Override
                    public <T> FieldType visitSet(final UseSet<T> type) {

                        return new FieldType.ArrayType(type.getType().visit(this));
                    }

                    @Override
                    public <T> FieldType visitMap(final UseMap<T> type) {

                        if(expand != null) {
                            // FIXME:
                            throw new UnsupportedOperationException();
                        }
                        return new FieldType.MapType(type.getType().visit(this));
                    }

                    @Override
                    public FieldType visitStruct(final UseStruct type) {

                        return new FieldType.NestedType(properties(type.getSchema(), expand));
                    }

                    @Override
                    public FieldType visitBinary(final UseBinary type) {

                        return FieldType.BINARY;
                    }

                    @Override
                    public FieldType visitDate(final UseDate type) {

                        return FieldType.DATE;
                    }

                    @Override
                    public FieldType visitDateTime(final UseDateTime type) {

                        return FieldType.DATETIME;
                    }

                    @Override
                    public FieldType visitView(final UseView type) {

                        return new FieldType.NestedType(properties(type.getSchema(), expand));
                    }

                    @Override
                    public <T> FieldType visitOptional(final UseOptional<T> type) {

                        return type.getType().visit(this);
                    }

                    @Override
                    public FieldType visitAny(final UseAny type) {

                        throw new UnsupportedOperationException();
                    }
                });
            }
        }
    }
}
