package io.basestar.expression.text;

import io.basestar.expression.Expression;
import lombok.Data;

/**
 * Like
 *
 * Case-sensitive wildcard match.
 *
 * The supported syntax is as-per standard SQL with ESCAPE \
 */

@Data
public class SLike implements Like {

    public static final String TOKEN = "LIKE";

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs LIKE rhs
     *
     * @param lhs string Left hand operand
     * @param rhs string Right hand operand
     */

    public SLike(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isCaseSensitive() {

        return true;
    }

    @Override
    public Like create(final Expression lhs, final Expression rhs) {

        return new SLike(lhs, rhs);
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public String toString() {

        return lhs + " " + TOKEN + " " + rhs;
    }
}
