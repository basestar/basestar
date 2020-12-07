package io.basestar.codegen.model;

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

import io.basestar.codegen.CodegenContext;
import io.basestar.mapper.annotation.Description;
import io.basestar.schema.Index;
import io.basestar.schema.InterfaceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.util.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class InterfaceSchemaModel extends InstanceSchemaModel {

    private final InterfaceSchema schema;

    public InterfaceSchemaModel(final CodegenContext context, final InterfaceSchema schema) {

        super(context, schema);
        this.schema = schema;
    }

    @Override
    public String getSchemaType() {

        return InterfaceSchema.Descriptor.TYPE;
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel<>(getContext(), VALID));
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.InterfaceSchema.Declaration.annotation(schema)));
        for(final Index index : schema.getIndexes().values()) {
            annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.Index.Modifier.annotation(index)));
        }
        if(schema.getDescription() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Description.Modifier.annotation(schema.getDescription())));
        }
        return annotations;
    }

    @Override
    public List<MemberModel> getAdditionalMembers() {

        return Stream.concat(
                schema.getExtend() != null ? Stream.<MemberModel>empty() : schema.metadataSchema().entrySet().stream()
                        .filter(entry -> !ObjectSchema.SCHEMA.equals(entry.getKey()) && !entry.getKey().startsWith(Reserved.PREFIX))
                        .map(entry -> new MetadataModel(getContext(), entry.getKey(), entry.getValue())),
                schema.getDeclaredLinks().values().stream()
                        .map(v -> new LinkModel(getContext(), v))).collect(Collectors.toList());
    }

    @Override
    public List<InstanceSchemaModel> getExtend() {

        final CodegenContext context = getContext();
        return Immutable.transform(schema.getExtend(), v -> from(context, v));
    }
}
