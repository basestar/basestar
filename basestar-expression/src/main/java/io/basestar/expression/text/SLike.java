package io.basestar.expression.text;

import io.basestar.expression.Expression;
import lombok.Data;

@Data
public class SLike implements Like {

    public static final String TOKEN = "LIKE";

    private final Expression lhs;

    private final Expression rhs;

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
