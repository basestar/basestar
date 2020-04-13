package io.basestar.graphql.wiring;

/*-
 * #%L
 * basestar-graphql
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

import com.google.common.io.BaseEncoding;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.use.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataConverter {

    @SuppressWarnings("unchecked")
    public Map<String, Object> toResponse(final InstanceSchema schema, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.metadataSchema().forEach((k, use) -> result.put(k, toResponse(use, input.get(k))));
            schema.getAllProperties().forEach((k, prop) -> result.put(k, toResponse(prop.getType(), input.get(k))));
            if (schema instanceof Link.Resolver) {
                ((Link.Resolver) schema).getAllLinks().forEach((k, link) -> {
                    final List<Map<String, Object>> values = (List<Map<String, Object>>) input.get(k);
                    if (values != null) {
                        result.put(k, values.stream().map(value -> toResponse(link.getSchema(), value))
                                .collect(Collectors.toList()));
                    }
                });
            }
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> fromRequest(final InstanceSchema schema, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.getAllProperties().forEach((k, prop) -> result.put(k, fromRequest(prop.getType(), input.get(k))));
            return result;
        }
    }

    protected Object toResponse(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitRef(final UseRef type) {

                    return toResponse(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitArray(final UseArray<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), v))
                            .collect(Collectors.toList());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitSet(final UseSet<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), v))
                            .collect(Collectors.toSet());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitMap(final UseMap<T> type) {

                    return ((Map<String, ?>)value).entrySet().stream()
                            .map(e -> {
                                final Map<String, Object> result = new HashMap<>();
                                result.put(TypeDefinitionRegistryFactory.MAP_KEY, e.getKey());
                                result.put(TypeDefinitionRegistryFactory.MAP_VALUE, toResponse(type.getType(), e.getValue()));
                                return result;
                            })
                            .collect(Collectors.toList());
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    return toResponse(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    return BaseEncoding.base64().encode(type.create(value));
                }
            });
        }
    }

    protected Object fromRequest(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitRef(final UseRef type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitArray(final UseArray<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> fromRequest(type.getType(), v))
                            .collect(Collectors.toList());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitSet(final UseSet<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> fromRequest(type.getType(), v))
                            .collect(Collectors.toSet());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitMap(final UseMap<T> type) {

                    final Map<String, Object> result = new HashMap<>();
                    ((Collection<Map<String, ?>>)value).forEach(v -> {
                        final String key = (String)v.get(TypeDefinitionRegistryFactory.MAP_KEY);
                        final Object value = fromRequest(type.getType(), v.get(TypeDefinitionRegistryFactory.MAP_VALUE));
                        result.put(key, value);
                    });
                    return result;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    return fromRequest(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    return type.create(BaseEncoding.base64().decode(value.toString()));
                }
            });
        }
    }
}
