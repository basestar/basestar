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

import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Property;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class PropertyModel extends MemberModel {

    private final Property property;

    public PropertyModel(final CodegenSettings settings, final Property property) {

        super(settings);

        this.property = property;
    }

    @Override
    public String getName() {

        return property.getName();
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        final List<AnnotationModel> annotations = new ArrayList<>();
        final ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
        values.put("name", property.getName());
        if(property.isRequired()) {
            values.put("required", true);
        }
        if(property.isImmutable()) {
            values.put("immutable", true);
        }
        if(property.getExpression() != null) {
            values.put("expression", property.getExpression().toString());
        }
        annotations.add(new AnnotationModel(getSettings(), io.basestar.mapper.annotation.Property.class, values.build()));
        if(property.isRequired()) {
            annotations.add(new AnnotationModel(getSettings(), javax.validation.constraints.NotNull.class, ImmutableMap.of()));
        }
        return annotations;
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), property.getType());
    }

    @Override
    public boolean isRequired() {

        return property.isRequired();
    }
}
