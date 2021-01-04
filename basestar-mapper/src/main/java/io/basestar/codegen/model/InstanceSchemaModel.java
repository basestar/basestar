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
import io.basestar.codegen.Codebehind;
import io.basestar.codegen.CodegenContext;
import io.basestar.schema.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public abstract class InstanceSchemaModel extends SchemaModel {

    protected final InstanceSchema schema;

    public InstanceSchemaModel(final CodegenContext context, final InstanceSchema schema) {

        super(context, schema);
        this.schema = schema;
    }

    public List<MemberModel> getMembers() {

        return Stream.concat(
                schema.getDeclaredProperties().values().stream()
                        .map(v -> new PropertyModel(getContext(), v)),
                getAdditionalMembers().stream()
        ).sorted(Comparator.comparing(MemberModel::getName)).collect(Collectors.toList());
    }

    protected List<MemberModel> getAdditionalMembers() {

        return ImmutableList.of();
    }

    public boolean isAbstract() {

        return !schema.isConcrete();
    }

    public List<InstanceSchemaModel> getExtend() {

        return Collections.emptyList();
    }

    protected static InstanceSchemaModel from(final CodegenContext context, final InstanceSchema schema) {

        final Codebehind codebehind = context.getCodebehind().get(schema.getQualifiedName());
        if(codebehind != null) {
            return new CodebehindModel(context, codebehind, schema);
        } else if (schema instanceof ObjectSchema) {
            return new ObjectSchemaModel(context, (ObjectSchema) schema);
        } else if (schema instanceof InterfaceSchema) {
            return new InterfaceSchemaModel(context, (InterfaceSchema) schema);
        } else if (schema instanceof StructSchema) {
            return new StructSchemaModel(context, (StructSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return new ViewSchemaModel(context, (ViewSchema) schema);
        } else {
            return null;
        }
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>(super.getAnnotations());
        annotations.add(new AnnotationModel<>(getContext(), VALID));
        return annotations;
    }
}
