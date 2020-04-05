package io.basestar.mapper.type;

import java.util.List;

public interface HasTypeParameters {

    List<WithTypeVariable<?>> typeParameters();

}
