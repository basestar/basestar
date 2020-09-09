package io.basestar.mapper.internal;

/*-
 * #%L
 * basestar-mapper
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
import io.basestar.mapper.MappingContext;
import io.basestar.schema.*;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

import java.util.Map;

public class ObjectSchemaMapper<T> extends InstanceSchemaMapper<T, ObjectSchema.Builder> {

    private final Map<String, Index.Descriptor> indexes;

    private final Map<String, Permission.Descriptor> permissions;

    public ObjectSchemaMapper(final MappingContext context, final Name name, final TypeContext type) {

        super(ObjectSchema.Builder.class, context, name, type);
        this.indexes = ImmutableMap.of();
        this.permissions = ImmutableMap.of();

//        for(final AnnotationContext<Permission> annot : type.annotations(Permission.class)) {
//            final Permission perm = annot.annotation();
//            permissions.put(perm.on(), new io.basestar.schema.Permission.Builder()
//                    .setAnonymous(perm.anon())
//                    .setExpression(Expression.parse(perm.expression()))
//                    .setExpand(expand(perm.expand())));
//        }
    }

    private ObjectSchemaMapper(final ObjectSchemaMapper<T> copy, final String description, final Map<String, Index.Descriptor> indexes, final Map<String, Permission.Descriptor> permissions) {

        super(copy, description);
        this.indexes = indexes;
        this.permissions = permissions;
    }

    public ObjectSchemaMapper<T> withIndexes(final Map<String, Index.Descriptor> indexes) {

        return new ObjectSchemaMapper<>(this, description, ImmutableMap.<String, Index.Descriptor>builder().putAll(this.indexes).putAll(indexes).build(), permissions);
    }

    public ObjectSchemaMapper<T> withPermissions(final Map<String, Permission.Descriptor> permissions) {

        return new ObjectSchemaMapper<>(this, description, indexes, ImmutableMap.<String, Permission.Descriptor>builder().putAll(this.permissions).putAll(permissions).build());
    }

    @Override
    public ObjectSchemaMapper<T> withDescription(final String description) {

        return new ObjectSchemaMapper<>(this, description, indexes, permissions);
    }

    @Override
    public ObjectSchema.Builder schemaBuilder() {

        return addMembers(ObjectSchema.builder()
                .setConcrete(concrete ? null : false)
                .setExtend(extend)
                .setIndexes(indexes.isEmpty() ? null : indexes));
    }

    @Override
    public void addLink(final ObjectSchema.Builder builder, final String name, final Link.Builder link) {

        builder.setLink(name, link);
    }

    @Override
    public void addTransient(final ObjectSchema.Builder builder, final String name, final Transient.Builder trans) {

        builder.setTransient(name, trans);
    }
}
