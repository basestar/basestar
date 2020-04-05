package io.basestar.mapper.type;

import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class WithMethod<T, V> implements HasName, HasModifiers, HasAnnotations, HasParameters, HasType<V> {

    private final WithType<T> owner;

    private final Method method;

    private final AnnotatedType annotatedType;

    private final List<WithParameter<?>> parameters;

    private final List<WithAnnotation<?>> annotations;

    protected WithMethod(final WithType<T> owner, final Method method) {

        this.owner = owner;
        this.method = method;
        this.annotatedType = GenericTypeReflector.getReturnType(method, owner.annotatedType());
        this.parameters = WithParameter.from(owner.annotatedType(), method);
        this.annotations = WithAnnotation.from(method);
    }

    @Override
    public String name() {

        return method.getName();
    }

    @Override
    public int modifiers() {

        return method.getModifiers();
    }

    public V invoke(final T parent, final Object ... args) throws InvocationTargetException, IllegalAccessException {

        return erasedType().cast(method.invoke(parent, args));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> erasedType() {

        return (Class<V>)method.getReturnType();
    }
}
