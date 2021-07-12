//package io.basestar.schema.from;
//
//import io.basestar.expression.Expression;
//import io.basestar.expression.constant.NameConstant;
//import io.basestar.schema.Property;
//import io.basestar.schema.expression.InferenceContext;
//import io.basestar.schema.expression.TypedExpression;
//import io.basestar.schema.use.Use;
//import io.basestar.util.Immutable;
//import io.basestar.util.Name;
//import io.basestar.util.Nullsafe;
//import io.basestar.util.Sort;
//import lombok.AccessLevel;
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.Getter;
//
//import javax.annotation.Nonnull;
//import javax.annotation.Nullable;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public abstract class AbstractFrom implements From {
//
//    @Nonnull
//    @Getter(AccessLevel.PROTECTED)
//    private final Arguments arguments;
//
//    public AbstractFrom(final From.Descriptor from) {
//
//        this.arguments = new Arguments(from);
//    }
//
//    public AbstractFrom(@Nonnull final Arguments arguments) {
//
//        this.arguments = arguments;
//    }
//
//    public AbstractFrom() {
//
//        this.arguments = new Arguments();
//    }
//
//    protected abstract AbstractFrom with(final Arguments arguments);
//
//    @Override
//    public String getAs() {
//
//        return arguments.getAs();
//    }
//
//    @Override
//    public List<Sort> getSort() {
//
//        return arguments.getSort();
//    }
//
//    @Override
//    public Map<String, Expression> getSelect() {
//
//        return arguments.getSelect();
//    }
//
//    @Override
//    public Expression getWhere() {
//
//        return arguments.getWhere();
//    }
//
//    @Override
//    public List<String> getGroup() {
//
//        return arguments.getGroup();
//    }
//
//    @Override
//    public From as(final String as) {
//
//        return with(arguments.as(as));
//    }
//
//    @Override
//    public From select(final Map<String, Expression> select) {
//
//        return with(arguments.select(select));
//    }
//
//    @Override
//    public From where(final Expression where) {
//
//        return with(arguments.where(where));
//    }
//
//    @Override
//    public From group(final List<String> group) {
//
//        return with(arguments.group(group));
//    }
//
//    protected abstract InferenceContext undecoratedInferenceContext();
//
//    @Override
//    public InferenceContext inferenceContext() {
//
//        final InferenceContext context = undecoratedInferenceContext();
//        final Map<String, Expression> select = getSelect();
//        if(select == null || select.isEmpty()) {
//            return context;
//        } else {
//            return InferenceContext.from(Immutable.transformValues(select, (k, v) -> context.typeOf(v)));
//        }
//    }
//
//    protected abstract Map<String, Use<?>> getUndecoratedProperties();
//
//    public Map<String, Use<?>> getProperties() {
//
//        final Map<String, Expression> select = getSelect();
//        if(select != null) {
//            final InferenceContext context = undecoratedInferenceContext();
//            return Immutable.transformValues(select, (k, v) -> context.typeOf(v));
//        } else {
//            return getUndecoratedProperties();
//        }
//    }
//
////    @Override
////    public Map<String, TypedExpression<?>> selectExpressions() {
////
////        final Map<String, Expression> select = getSelect();
////        if(select != null) {
////            final InferenceContext context = undecoratedInferenceContext();
////            return Immutable.transformValues(select, (k, v) -> TypedExpression.from(v, context.typeOf(v)));
////        } else {
////            return Immutable.transformValues(getProperties(), (k, v) -> TypedExpression.from(new NameConstant(k), v));
////        }
////    }
//
//    public static abstract class Descriptor implements From.Descriptor {
//
//        private final Arguments arguments;
//
//        public Descriptor(final Arguments from) {
//
//            this.arguments = from;
//        }
//
//        @Override
//        public List<Sort> getSort() {
//
//            return arguments.getSort();
//        }
//
//        @Override
//        public Map<String, Expression> getSelect() {
//
//            return arguments.getSelect();
//        }
//
//        @Override
//        public Expression getWhere() {
//
//            return arguments.getWhere();
//        }
//
//        @Override
//        public List<String> getGroup() {
//
//            return arguments.getGroup();
//        }
//
//        @Override
//        public String getAs() {
//
//            return arguments.getAs();
//        }
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class Arguments {
//
//        @Nonnull
//        private final Map<String, Expression> select;
//
//        @Nullable
//        private final Expression where;
//
//        @Nonnull
//        private final List<String> group;
//
//        @Nonnull
//        private final List<Sort> sort;
//
//        private final String as;
//
//        public Arguments(final From.Descriptor from) {
//
//            this.select = Immutable.map(from.getSelect());
//            this.where = from.getWhere();
//            this.group = Immutable.list(from.getGroup());
//            this.sort = Immutable.list(from.getSort());
//            this.as = from.getAs();
//        }
//
//        public Arguments() {
//
//            this.select = Immutable.map();
//            this.where = null;
//            this.group = Immutable.list();
//            this.sort = Immutable.list();
//            this.as = null;
//        }
//
//        public Arguments as(final String as) {
//
//            return new Arguments(select, where, group, sort, as);
//        }
//
//        public Arguments select(final Map<String, Expression> select) {
//
//            return new Arguments(select, where, group, sort, as);
//        }
//
//        public Arguments where(final Expression where) {
//
//            return new Arguments(select, where, group, sort, as);
//        }
//
//        public Arguments group(final List<String> group) {
//
//            return new Arguments(select, where, group, sort, as);
//        }
//    }
//}
