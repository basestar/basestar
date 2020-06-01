package io.basestar.mapper.context.has;

import io.basestar.mapper.context.ConstructorContext;

import java.util.List;

public interface HasConstructors {

    List<ConstructorContext> declaredConstructors();

    List<ConstructorContext> constructors();
}
