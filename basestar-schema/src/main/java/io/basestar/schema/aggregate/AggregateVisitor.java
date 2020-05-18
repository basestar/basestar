package io.basestar.schema.aggregate;

public interface AggregateVisitor<T> {

    T visitSum(Sum aggregate);

    T visitMin(Min aggregate);

    T visitMax(Max aggregate);

    T visitAvg(Avg aggregate);

    T visitCount(Count count);

    interface Defaulting<T> extends AggregateVisitor<T> {

        T visitDefault(Aggregate aggregate);

        @Override
        default T visitSum(final Sum aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitMin(final Min aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitMax(final Max aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitAvg(final Avg aggregate) {

            return visitDefault(aggregate);
        }

        @Override
        default T visitCount(final Count aggregate) {

            return visitDefault(aggregate);
        }
    }
}
