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
import io.basestar.mapper.annotation.Expression;
import io.basestar.mapper.annotation.Immutable;
import io.basestar.schema.Property;

import java.util.ArrayList;
import java.util.List;

//import io.basestar.mapper.annotation.Required;

@SuppressWarnings("unused")
public class PropertyModel extends MemberModel {

    private final Property property;

    public PropertyModel(final CodegenContext context, final Property property) {

        super(context);

        this.property = property;
    }

    @Override
    public String getName() {

        return property.getName();
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.Property.Declaration.annotation(property)));
        if(property.getExpression() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Expression.Modifier.annotation(property.getExpression())));
        }
        if(property.isRequired()) {
            annotations.add(new AnnotationModel<>(getContext(), NOT_NULL));
//            annotations.add(new AnnotationModel<>(getSettings(), Required.Modifier.annotation(true)));
        }
        if(property.isImmutable()) {
            annotations.add(new AnnotationModel<>(getContext(), Immutable.Modifier.annotation(true)));
        }
        property.getConstraints().forEach(constraint -> annotations.add(new AnnotationModel<>(getContext(), constraint.toJsr380(property.getType()))));
        if(property.getDescription() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Description.Modifier.annotation(property.getDescription())));
        }
        return annotations;
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getContext(), property.getType());
    }

    @Override
    public boolean isRequired() {

        return property.isRequired();
    }
}
