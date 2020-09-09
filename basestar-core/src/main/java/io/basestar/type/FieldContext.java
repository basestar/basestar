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
import io.basestar.type.has.HasModifiers;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("Guava")
public class FieldContext implements HasModifiers, AccessorContext {

    private final TypeContext owner;

    private final Field field;

    private final Supplier<List<AnnotationContext<?>>> annotations;

    protected FieldContext(final TypeContext owner, final Field field) {

        this.owner = owner;
        this.field = field;
        this.annotations = Suppliers.memoize(() -> AnnotationContext.from(field));
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

        return !Modifier.isFinal(field.getModifiers());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, V> V get(final T target) throws IllegalAccessException {

        return (V)field.get(target);
    }

    @Override
    public <T, V> void set(final T target, final V value) throws IllegalAccessException {

        field.setAccessible(true);
        field.set(target, value);
    }

    @Override
    public SerializableAccessor serializableAccessor() {

        return serializableAccessor(field.getDeclaringClass(), name(), canSet());
    }

    private static SerializableAccessor serializableAccessor(final Class<?> erasedOwner, final String name, final boolean canSet) {

        return new SerializableAccessor() {
            @Override
            public boolean canGet() {

                return true;
            }

            @Override
            public boolean canSet() {

                return canSet;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T, V> V get(final T target) throws IllegalAccessException {

                try {
                    final Field field = erasedOwner.getDeclaredField(name);
                    field.setAccessible(true);
                    return (V) field.get(target);
                } catch (final NoSuchFieldException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public <T, V> void set(final T target, final V value) throws IllegalAccessException {

                try {
                    final Field field = erasedOwner.getDeclaredField(name);
                    field.setAccessible(true);
                    field.set(target, value);
                } catch (final NoSuchFieldException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    @Override
    public AnnotatedType annotatedType() {

        return GenericTypeReflector.getFieldType(field, owner.annotatedType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Class<V> erasedType() {

        return (Class<V>)field.getType();
    }

    @Override
    public List<AnnotationContext<?>> annotations() {

        return annotations.get();
    }
}
