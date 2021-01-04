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
import io.basestar.mapper.annotation.Group;
import io.basestar.mapper.annotation.Where;
import io.basestar.schema.ViewSchema;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class ViewSchemaModel extends LinkableSchemaModel {

    private final ViewSchema schema;

    public ViewSchemaModel(final CodegenContext context, final ViewSchema schema) {

        super(context, schema);
        this.schema = schema;
    }

    @Override
    public String getSchemaType() {

        return ViewSchema.Descriptor.TYPE;
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final ImmutableList.Builder<AnnotationModel<?>> annotations = ImmutableList.builder();
        annotations.addAll(super.getAnnotations());
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.ViewSchema.Declaration.annotation(schema)));
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.From.Modifier.annotation(schema.getFrom())));
        if(!schema.getGroup().isEmpty()) {
            annotations.add(new AnnotationModel<>(getContext(), Group.Modifier.annotation(schema.getGroup())));
        }
        if(schema.getWhere() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Where.Modifier.annotation(schema.getWhere())));
        }
        return annotations.build();
    }

    @Override
    public List<InstanceSchemaModel> getExtend() {

        return Collections.emptyList();
    }
}
