package io.basestar.mapper.type;

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
