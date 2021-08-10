package io.basestar.schema.from;

public interface FromVisitor<T> {

    T visitAgg(FromAgg from);

    T visitAlias(FromAlias from);

    T visitFilter(FromFilter from);

    T visitJoin(FromJoin from);

    T visitMap(FromMap from);

    T visitSchema(FromSchema from);

    T visitSort(FromSort from);

    T visitUnion(FromUnion from);

    T visitExternal(FromExternal from);
}
