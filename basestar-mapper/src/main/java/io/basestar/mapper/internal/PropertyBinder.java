package io.basestar.mapper.internal;

import io.basestar.mapper.type.WithProperty;

public interface PropertyBinder {

    String name(WithProperty<?, ?> property);
}
