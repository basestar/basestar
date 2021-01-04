package io.basestar.spark.hadoop;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.util.Ref;
import io.basestar.storage.Scan;
import io.basestar.storage.Storage;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StorageInputFormat extends InputFormat<Ref, Instance> {

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StorageSplit extends InputSplit implements Writable {

        private Name schema;

        private Expression expression;

        private int split;

        private int splits;

        @Override
        public void write(final DataOutput dataOutput) throws IOException {

            dataOutput.writeUTF(schema.toString());
            dataOutput.writeUTF(expression.toString());
            dataOutput.writeInt(split);
            dataOutput.writeInt(splits);
        }

        @Override
        public void readFields(final DataInput dataInput) throws IOException {

            schema = Name.parse(dataInput.readUTF());
            expression = Expression.parse(dataInput.readUTF());
            split = dataInput.readInt();
            splits = dataInput.readInt();
        }

        @Override
        public long getLength() {

            return 0;
        }

        @Override
        public String[] getLocations() {

            return new String[] { schema.toString() + ":" + split + ":" + splits };
        }
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException {

        final Configuration configuration = job.getConfiguration();
        final StorageProvider provider = StorageProvider.provider(configuration);
        final Namespace namespace = provider.namespace(configuration);
        final ReferableSchema schema = namespace.requireReferableSchema(provider.schema(configuration));
        final Expression expression = provider.inputQuery(configuration);
        final int attemptSplits = provider.inputSplits(configuration);
        final Storage storage = provider.storage(configuration);
        final Scan scan = storage.scan(schema, expression, attemptSplits);
        final int splits = scan.getSegments();
        final List<InputSplit> result = new ArrayList<>();
        for(int split = 0; split != splits; ++split) {
            result.add(new StorageSplit(schema.getQualifiedName(), expression, split, splits));
        }
        return result;
    }

    @Override
    public RecordReader<Ref, Instance> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext attempt) throws IOException {

        final Configuration configuration = attempt.getConfiguration();
        final StorageProvider provider = StorageProvider.provider(configuration);
        final Storage storage = provider.storage(configuration);
        final Namespace namespace = provider.namespace(configuration);
        final StorageSplit storageSplit = (StorageSplit)inputSplit;
        final ReferableSchema schema = namespace.requireReferableSchema(storageSplit.getSchema());
        final Expression expression = storageSplit.getExpression();
        final Scan.Segment segment = storage.scan(schema, expression, storageSplit.getSplits()).segment(storageSplit.getSplit());
        return new RecordReader<Ref, Instance>() {

            private Instance next;

            @Override
            public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) {

                // no implementation required
            }

            @Override
            public boolean nextKeyValue() {

                while (segment.hasNext()) {
                    next = new Instance(segment.next());
                    if(expression.evaluatePredicate(Context.init(next))) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Ref getCurrentKey() {

                return Ref.of(schema.getQualifiedName(), next.getId());
            }

            @Override
            public Instance getCurrentValue() {

                return next;
            }

            @Override
            public float getProgress() {

                return 0;
            }

            @Override
            public void close() throws IOException {

                segment.close();
            }
        };
    }
}
