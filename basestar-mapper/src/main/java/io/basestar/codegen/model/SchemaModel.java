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

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Schema;
import io.basestar.util.Text;

import java.util.List;

public abstract class SchemaModel extends Model {

    private final Schema<?> schema;

    public SchemaModel(final CodegenSettings settings, final Schema<?> schema) {

        super(settings);
        this.schema = schema;
    }

    public String getClassName() {

        return Text.upperCamel(getName());
    }

    public String getName() {

        return schema.getSimpleName();
    }

    public String getDescription() {

        return schema.getDescription();
    }

//    public String getAnnotationClassName() {
//
//        return getAnnotationClass().getName();
//    }
//
//    protected abstract Class<?> getAnnotationClass();
//
//    public Map<String, Object> getAnnotationValues() {
//
//        return ImmutableMap.of("name", schema.getName());
//    }

    public abstract List<AnnotationModel> getAnnotations();
}
