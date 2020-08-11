package io.basestar.spark.transform;

import io.basestar.schema.layout.Layout;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

public class LayoutTransform implements DatasetMapTransform {

    private final RowTransformImpl rowTransform;

    @lombok.Builder(builderClassName = "Builder")
    LayoutTransform(final Layout inputLayout, final Layout outputLayout, final Set<Name> expand) {

        this.rowTransform = new RowTransformImpl(inputLayout, outputLayout, expand);
    }

    @Override
    public RowTransform rowTransform() {

        return rowTransform;
    }

    @AllArgsConstructor
    private static class RowTransformImpl implements RowTransform {

        private final Layout inputLayout;

        private final Layout outputLayout;

        private final Set<Name> expand;

        private final StructType outputStructType;

        RowTransformImpl(final Layout inputLayout, final Layout outputLayout, final Set<Name> expand) {

            this.inputLayout = inputLayout;
            this.outputLayout = outputLayout;
            this.expand = Nullsafe.option(expand);
            this.outputStructType = SparkSchemaUtils.structType(this.outputLayout, this.expand);
        }

        @Override
        public StructType schema(final StructType input) {

            return outputStructType;
        }

        @Override
        public Row accept(final Row input) {

            final Map<String, Object> initial = SparkSchemaUtils.fromSpark(inputLayout, expand, input);
            final Map<String, Object> canonical = inputLayout.unapplyLayout(expand, initial);
            final Map<String, Object> result = outputLayout.applyLayout(expand, canonical);
            return SparkSchemaUtils.toSpark(outputLayout, expand, outputStructType, result);
        }
    }
}
