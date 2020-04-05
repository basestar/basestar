package io.basestar.mapper.type;

import java.util.List;

public interface HasMethods<T> {

    List<WithMethod<T, ?>> declaredMethods();

    List<WithMethod<? super T, ?>> methods();
}
