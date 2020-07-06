package io.basestar.expression.text;

import io.basestar.expression.Expression;
import lombok.Data;

@Data
public class ILike implements Like {

    public static final String TOKEN = "ILIKE";

    private final Expression lhs;

    private final Expression rhs;

    public ILike(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public boolean isCaseSensitive() {

        return false;
    }

    @Override
    public ILike create(final Expression lhs, final Expression rhs) {

        return new ILike(lhs, rhs);
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
