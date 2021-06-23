package io.basestar.expression.sql;

import io.basestar.expression.Expression;
import lombok.Data;

public interface Union {

    @Data
    class Distinct implements Union {

        private final Expression expr;
    }

    @Data
    class All implements Union {

        private final Expression expr;
    }
}
