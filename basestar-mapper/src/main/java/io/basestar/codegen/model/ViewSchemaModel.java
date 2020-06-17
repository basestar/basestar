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
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.ViewSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ViewSchemaModel extends InstanceSchemaModel {

    private final ViewSchema schema;

    public ViewSchemaModel(final CodegenSettings settings, final ViewSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        final Map<String, Object> values = new HashMap<>();
        values.put("name", schema.getName());
        values.put("from", schema.getFrom().getName());
        if(schema.getWhere() != null) {
            values.put("where", schema.getWhere().toString());
        }
        return ImmutableList.of(
                new AnnotationModel(getSettings(), javax.validation.Valid.class),
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.ViewSchema.class, values)
        );
    }

    @Override
    public InstanceSchemaModel getExtend() {

        return null;
    }
}
