package io.basestar.expression.type;

import lombok.Data;

import java.math.BigDecimal;

public interface DecimalContext {

    DecimalContext DEFAULT = new Default();

    @Data
    class PrecisionAndScale {

        private final int precision;

        private final int scale;
    }

    default PrecisionAndScale addition(final BigDecimal lhs, final BigDecimal rhs) {

        return addition(lhs.precision(), lhs.scale(), rhs.precision(), rhs.scale());
    }

    PrecisionAndScale addition(int p1, int s1, int p2, int s2);

    default PrecisionAndScale multiplication(final BigDecimal lhs, final BigDecimal rhs) {

        return multiplication(lhs.precision(), lhs.scale(), rhs.precision(), rhs.scale());
    }

    PrecisionAndScale multiplication(int p1, int s1, int p2, int s2);

    default PrecisionAndScale division(final BigDecimal lhs, final BigDecimal rhs) {

        return division(lhs.precision(), lhs.scale(), rhs.precision(), rhs.scale());
    }

    PrecisionAndScale division(int p1, int s1, int p2, int s2);

    // See: https://docs.cloudera.com/cdp-private-cloud-base/7.1.6/impala-sql-reference/topics/impala-decimal.html

    class Default implements DecimalContext {

        public static final int MIN_DIVISION_SCALE = 6;

        @Override
        public PrecisionAndScale addition(final int p1, final int s1, final int p2, final int s2) {

            final int l1 = p1 - s1;
            final int l2 = p2 - s2;
            final int p = Math.max(l1, l2) + Math.max(s1, s2) + 1;
            final int s = Math.max(s1, s2);
            return new PrecisionAndScale(p, s);
        }

        @Override
        public PrecisionAndScale multiplication(final int p1, final int s1, final int p2, final int s2) {

            final int p = p1 + p2 + 1;
            final int s = s1 + s2;
            return new PrecisionAndScale(p, s);
        }

        @Override
        public PrecisionAndScale division(final int p1, final int s1, final int p2, final int s2) {

            final int l1 = p1 - s1;
            final int p = l1 + s2 + Math.max(s1 + p2 + 1, MIN_DIVISION_SCALE);
            final int s = Math.max(s1 + p2 + 1, MIN_DIVISION_SCALE);
            return new PrecisionAndScale(p, s);
        }
    }
}