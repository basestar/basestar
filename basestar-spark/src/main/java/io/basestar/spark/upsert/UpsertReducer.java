package io.basestar.spark.upsert;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

class UpsertReducer extends UserDefinedAggregateFunction {

    private final StructType inputSchema;

    private final int sequenceOffset;

    public UpsertReducer(final StructType inputSchema) {

        this.inputSchema = inputSchema;
        this.sequenceOffset = inputSchema.fieldIndex(UpsertTable.SEQUENCE);
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

        return true;
    }

    @Override
    public void initialize(final MutableAggregationBuffer buffer) {

        buffer.update(sequenceOffset, UpsertTable.NO_SEQUENCE);
    }

    @Override
    public void update(final MutableAggregationBuffer buffer, final Row row) {

        final String current = (String)buffer.get(sequenceOffset);
        final String incoming = (String)row.get(sequenceOffset);
        if(incoming.compareTo(current) >= 0) {
            final int size = row.size();
            for(int i = 0; i != size; ++i) {
                buffer.update(i, row.get(i));
            }
        }
    }

    @Override
    public void merge(final MutableAggregationBuffer buffer, final Row row) {

        update(buffer, row);
    }

    @Override
    public Object evaluate(final Row buffer) {

        return buffer;
    }
}
