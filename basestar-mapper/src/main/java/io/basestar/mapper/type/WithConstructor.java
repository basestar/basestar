package io.basestar.mapper.type;

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

import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class WithConstructor<T> implements HasModifiers, HasAnnotations, HasParameters {

    private final WithType<T> owner;

    private final Constructor<T> constructor;

    private final List<WithParameter<?>> parameters;

    private final List<WithAnnotation<?>> annotations;

    protected WithConstructor(final WithType<T> owner, final Constructor<T> constructor) {

        this.owner = owner;
        this.constructor = constructor;
        this.parameters = WithParameter.from(owner.annotatedType(), constructor);
        this.annotations = WithAnnotation.from(constructor);
    }

    public T newInstance(final Object ... args) throws InvocationTargetException, IllegalAccessException, InstantiationException {

        return constructor.newInstance(args);
    }

    @Override
    public int modifiers() {

        return constructor.getModifiers();
    }
}
