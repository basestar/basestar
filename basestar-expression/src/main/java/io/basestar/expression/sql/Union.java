package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import lombok.Data;

public interface Union {

    <T> T visit(Visitor<T> visitor);

    Union bind(Context context, Renaming root);

    @Data
    class Distinct implements Union {

        private final Expression expr;

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitDistinct(this);
        }

        @Override
        public Union bind(final Context context, final Renaming root) {

            final Expression expr = this.expr.bind(context, root);
            return expr == this.expr ? this : new Distinct(expr);
        }

        @Override
        public String toString() {

            return "UNION DISTINCT " + expr;
        }
    }

    @Data
    class All implements Union {

        private final Expression expr;

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAll(this);
        }

        @Override
        public Union bind(final Context context, final Renaming root) {

            final Expression expr = this.expr.bind(context, root);
            return expr == this.expr ? this : new All(expr);
        }

        @Override
        public String toString() {

            return "UNION ALL " + expr;
        }
    }

    interface Visitor<T> {

        T visitDistinct(Distinct from);

        T visitAll(All from);
    }
}
