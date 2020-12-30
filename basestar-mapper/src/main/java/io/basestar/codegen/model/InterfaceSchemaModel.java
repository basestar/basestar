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
import io.basestar.schema.Index;
import io.basestar.schema.InterfaceSchema;
import io.basestar.util.Immutable;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class InterfaceSchemaModel extends LinkableSchemaModel {

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

        final List<AnnotationModel<?>> annotations = new ArrayList<>(super.getAnnotations());
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.InterfaceSchema.Declaration.annotation(schema)));
        for(final Index index : schema.getIndexes().values()) {
            annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.Index.Modifier.annotation(index)));
        }
        return annotations;
    }

    @Override
    public List<InstanceSchemaModel> getExtend() {

        final CodegenContext context = getContext();
        return Immutable.transform(schema.getExtend(), v -> from(context, v));
    }
}
