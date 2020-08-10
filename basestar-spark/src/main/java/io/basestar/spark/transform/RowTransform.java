package io.basestar.spark.transform;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public interface RowTransform extends Transform<Row, Row>, Serializable {

    StructType schema(StructType input);

    default RowTransform then(final RowTransform next) {

        return new RowTransform() {
            @Override
            public StructType schema(final StructType input) {

                return next.schema(input);
            }

            @Override
            public Row accept(final Row input) {

                return next.accept(RowTransform.this.accept(input));
            }
        };
    }
}
