package io.basestar.type.has;

import io.basestar.type.ConstructorContext;

import java.util.List;

public interface HasConstructors {

    List<ConstructorContext> declaredConstructors();

    List<ConstructorContext> constructors();
}
