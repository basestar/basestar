package io.basestar.expression.bitwise;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Unary;
import io.basestar.expression.arithmetic.Negate;
import io.basestar.expression.type.match.UnaryMatch;
import io.basestar.expression.type.match.UnaryNumberMatch;
import lombok.Data;

/**
 * Bitwise Not
 */

@Data
public class BitNot implements Unary {

    public static final String TOKEN = "~";

    public static final int PRECEDENCE = Negate.PRECEDENCE + 1;

    private final Expression operand;

    /**
     * ~operand
     *
     * @param operand integer Operand
     */

    public BitNot(final Expression operand) {

        this.operand = operand;
    }

    @Override
    public Expression create(final Expression operand) {

        return new BitNot(operand);
    }

    @Override
    public Long evaluate(final Context context) {

        return VISITOR.apply(operand.evaluate(context));
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitBitFlip(this);
    }

    @Override
    public String toString() {

        return Unary.super.toString(operand);
    }

    private static final UnaryNumberMatch<Long> NUMBER_VISITOR = new UnaryNumberMatch<Long>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Long apply(final Long value) {

            return ~value;
        }
    };

    private static final UnaryMatch<Long> VISITOR = new UnaryMatch<Long>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Long apply(final Number value) {

            return NUMBER_VISITOR.apply(value);
        }
    };
}
