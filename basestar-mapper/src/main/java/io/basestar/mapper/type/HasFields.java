package io.basestar.mapper.type;

import java.util.List;

public interface HasFields<T> {

    List<WithField<T, ?>> declaredFields();
    
    List<WithField<? super T, ?>> fields();
}
