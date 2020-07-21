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

import com.google.common.collect.ImmutableList;
import io.basestar.codegen.CodegenContext;
import io.basestar.mapper.annotation.Description;
import io.basestar.schema.StructSchema;

import java.util.List;

@SuppressWarnings("unused")
public class StructSchemaModel extends InstanceSchemaModel {

    private final StructSchema schema;

    public StructSchemaModel(final CodegenContext context, final StructSchema schema) {

        super(context, schema);
        this.schema = schema;
    }

    @Override
    public String getSchemaType() {

        return StructSchema.Descriptor.TYPE;
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final ImmutableList.Builder<AnnotationModel<?>> annotations = ImmutableList.builder();
        annotations.add(new AnnotationModel<>(getContext(), VALID));
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.StructSchema.Declaration.annotation(schema)));
        if(schema.getDescription() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Description.Modifier.annotation(schema.getDescription())));
        }
        return annotations.build();
    }
}
