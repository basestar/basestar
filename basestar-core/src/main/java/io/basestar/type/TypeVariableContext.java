package io.basestar.type;

/*-
 * #%L
 * basestar-core
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

import io.basestar.type.has.HasName;
import io.basestar.type.has.HasType;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.TypeVariable;

@Getter
@Accessors(fluent = true)
public class TypeVariableContext implements HasName, HasType {

    private final TypeVariable<? extends Class<?>> variable;

    private final AnnotatedType annotatedType;

    protected TypeVariableContext(final TypeVariable<? extends Class<?>> variable, final AnnotatedType annotatedType) {

        this.variable = variable;
        if(annotatedType == null) {
            this.annotatedType = GenericTypeReflector.annotate(Object.class);
        } else {
            this.annotatedType = annotatedType;
        }
    }

    @Override
    public String name() {

        return variable.getName();
    }
}
