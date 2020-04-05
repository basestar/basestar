package io.basestar.mapper.type;

import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class WithParameter<V> implements HasName, HasModifiers, HasAnnotations, HasType<V> {

    private final Parameter parameter;

    private final AnnotatedType annotatedType;

    private final List<WithAnnotation<?>> annotations;

    protected WithParameter(final Parameter parameter, final AnnotatedType annotatedType) {

        this.parameter = parameter;
        this.annotatedType = annotatedType;
        this.annotations = WithAnnotation.from(parameter);
    }

    @Override
    public String name() {

        return parameter.getName();
    }

    @Override
    public int modifiers() {

        return parameter.getModifiers();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> erasedType() {

        return (Class<V>)parameter.getType();
    }

    protected static List<WithParameter<?>> from(final AnnotatedType type, final Executable exe) {

        final AnnotatedType[] types = GenericTypeReflector.getParameterTypes(exe, type);
        final java.lang.reflect.Parameter[] params = exe.getParameters();
        assert types.length == params.length;
        final WithParameter<?>[] result = new WithParameter<?>[types.length];
        for(int i = 0; i != types.length; ++i) {
            result[i] = new WithParameter<>(params[i], types[i]);
        }
        return Arrays.asList(result);
    }
}
