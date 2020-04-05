package io.basestar.mapper.type;

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
