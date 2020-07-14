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

import java.util.Collections;
import java.util.Map;

@SuppressWarnings("unused")
public class AnnotationModel extends Model {

    private final Class<?> cls;

    private final Map<String, Object> values;

    public AnnotationModel(final CodegenSettings settings, final Class<?> cls, final Map<String, Object> values) {

        super(settings);
        this.cls = cls;
        this.values = values;
    }

    public AnnotationModel(final CodegenSettings settings, final Class<?> cls) {

        this(settings, cls, Collections.emptyMap());
    }

    public String getClassName() {

        return cls.getName();
    }

    public Map<String, Object> getValues() {

        return values;
    }
}
