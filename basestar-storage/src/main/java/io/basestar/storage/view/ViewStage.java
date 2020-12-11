package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.schema.use.Use;
import lombok.RequiredArgsConstructor;

public interface ViewStage {

    @RequiredArgsConstructor
    class Field {

        private final Expression expression;

        private final Use<?> type;
    }
}
