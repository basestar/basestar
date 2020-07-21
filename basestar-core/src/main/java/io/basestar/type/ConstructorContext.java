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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.basestar.type.has.HasAnnotations;
import io.basestar.type.has.HasModifiers;
import io.basestar.type.has.HasParameters;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("Guava")
public class ConstructorContext implements HasModifiers, HasAnnotations, HasParameters {

    private final TypeContext owner;

    private final Constructor<?> constructor;

    private final Supplier<List<AnnotationContext<?>>> annotations;

    private final Supplier<List<ParameterContext>> parameters;

    protected ConstructorContext(final TypeContext owner, final Constructor<?> constructor) {

        this.owner = owner;
        this.constructor = constructor;
        this.annotations = Suppliers.memoize(() -> AnnotationContext.from(constructor));
        this.parameters = Suppliers.memoize(() -> ParameterContext.from(owner.annotatedType(), constructor));
    }

    @SuppressWarnings("unchecked")
    public <T> T newInstance(final Object ... args) throws InvocationTargetException, IllegalAccessException, InstantiationException {

        constructor.setAccessible(true);
        return (T)constructor.newInstance(args);
    }

    @Override
    public int modifiers() {

        return constructor.getModifiers();
    }

    @Override
    public List<AnnotationContext<?>> annotations() {

        return annotations.get();
    }

    @Override
    public List<ParameterContext> parameters() {

        return parameters.get();
    }
}
