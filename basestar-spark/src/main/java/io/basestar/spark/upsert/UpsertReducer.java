package io.basestar.spark.upsert;

import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

class UpsertReducer extends UserDefinedAggregateFunction {

    private final StructType inputSchema;

    private final int sequenceOffset;

    @Nullable
    private final Integer versionOffset;

    public UpsertReducer(final StructType inputSchema, final String versionColumn) {

        this.inputSchema = inputSchema;
        this.sequenceOffset = inputSchema.fieldIndex(UpsertTable.SEQUENCE);
        this.versionOffset = Nullsafe.map(versionColumn, inputSchema::fieldIndex);
    }

    @Override
    public StructType inputSchema() {

        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {

        return inputSchema;
    }

    @Override
    public DataType dataType() {

        return inputSchema;
    }

    @Override
    public boolean deterministic() {

        return false;
    }

    @Override
    public void initialize(final MutableAggregationBuffer buffer) {

        buffer.update(sequenceOffset, UpsertTable.EMPTY_PARTITION);
        if(versionOffset != null) {
            buffer.update(versionOffset, -1L);
        }
    }

    @Override
    public void update(final MutableAggregationBuffer buffer, final Row row) {

        final int size = row.size();
        for(int i = 0; i != size; ++i) {
            buffer.update(i, row.get(i));
        }
    }

    private boolean isLater(final MutableAggregationBuffer buffer, final Row row) {

        int cmp = compare(buffer, row, sequenceOffset);
        if(cmp == 0) {
            cmp = compare(buffer, row, versionOffset);
        }
        return cmp >= 0;
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> int compare(final MutableAggregationBuffer buffer, final Row row, final Integer field) {

        if(field == null) {
            return 0;
        } else {
            final T current = (T) buffer.get(field);
            final T incoming = (T) row.get(field);
            if(incoming == null) {
                return current == null ? 0 : -1;
            } else if(current == null) {
                return 1;
            } else {
                return incoming.compareTo(current);
            }
        }
    }

    @Override
    public void merge(final MutableAggregationBuffer buffer, final Row row) {

        if(isLater(buffer, row)) {
            update(buffer, row);
        }
    }

    @Override
    public Object evaluate(final Row buffer) {

        return buffer;
    }
}
