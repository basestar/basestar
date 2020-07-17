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

import java.util.List;

@SuppressWarnings("unused")
public abstract class MemberModel extends Model {

    public MemberModel(final CodegenSettings settings) {

        super(settings);
    }

    public abstract String getName();

    public String getFieldName() {

        return getName();
    }

    public abstract List<AnnotationModel<?>> getAnnotations();

    public abstract TypeModel getType();

    public abstract boolean isRequired();
}
