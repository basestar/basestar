package io.basestar.storage.view;

import io.basestar.schema.Layout;

public interface QueryStage {

    Layout outputLayout();

    <T> T visit(Visitor<T> visitor);

    interface Visitor<T> {

        T visitAgg(AggStage stage);

        T visitEmpty(EmptyStage stage);

        T visitExpand(ExpandStage stage);

        T visitFilter(FilterStage stage);

        T visitMap(MapStage stage);

        T visitSort(SortStage stage);

        T visitSource(SourceStage stage);

        T visitUnion(UnionStage stage);

        T visitSchema(SchemaStage schemaStage);
    }
}
