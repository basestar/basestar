package io.basestar.mapper.type;

/*-
 * #%L
 * basestar-mapper
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class WithField<T, V> implements HasModifiers, WithAccessor<T, V> {

    private final WithType<T> owner;

    private final Field field;

    private final AnnotatedType annotatedType;

    private final List<WithAnnotation<?>> annotations;

    protected WithField(final WithType<T> owner, final Field field) {

        this.owner = owner;
        this.field = field;
        this.annotatedType = GenericTypeReflector.getFieldType(field, owner.annotatedType());
        this.annotations = WithAnnotation.from(field);
    }

    @Override
    public String name() {

        return field.getName();
    }

    @Override
    public int modifiers() {

        return field.getModifiers();
    }

    @Override
    public boolean canGet() {

        return true;
    }

    @Override
    public boolean canSet() {

        return Modifier.isFinal(field.getModifiers());
    }

    @Override
    public V get(final T parent) throws IllegalAccessException {

        return erasedType().cast(field.get(parent));
    }

    @Override
    public void set(final T parent, final V value) throws IllegalAccessException {

        field.set(parent, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> erasedType() {

        return (Class<V>)field.getType();
    }
}
