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
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.StructSchema;

import java.util.List;

public class StructSchemaModel extends InstanceSchemaModel {

    private final StructSchema schema;

    public StructSchemaModel(final CodegenSettings settings, final StructSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), javax.validation.Valid.class),
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.StructSchema.class, ImmutableMap.of("name", schema.getName()))
        );
    }

    @Override
    public StructSchemaModel getExtend() {

        final StructSchema extend = schema.getExtend();
        if(extend != null) {
            return new StructSchemaModel(getSettings(), extend);
        } else {
            return null;
        }
    }
}
