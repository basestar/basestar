package io.basestar.spark.hadoop;

import io.basestar.schema.Instance;
import io.basestar.schema.util.Ref;
import io.basestar.storage.Storage;
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

    String PROVIDER = "io.basestar.storage.provider";

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Split extends InputSplit implements Writable {

        private int split;

        private int splits;

        @Override
        public void write(final DataOutput dataOutput) throws IOException {

            dataOutput.writeInt(split);
            dataOutput.writeInt(splits);
        }

        @Override
        public void readFields(final DataInput dataInput) throws IOException {

            split = dataInput.readInt();
            splits = dataInput.readInt();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {

            return 1;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {

            return new String[] { split + ":" + splits };
        }
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException, InterruptedException {

        final Configuration configuration = job.getConfiguration();
        final StorageProvider provider = StorageProvider.provider(configuration);
        final int splits = provider.inputSplits(configuration);
        final List<InputSplit> result = new ArrayList<>();
        for(int split = 0; split != splits; ++split) {
            result.add(new Split(split, splits));
        }
        return result;
    }

    @Override
    public RecordReader<Ref, Instance> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext attempt) throws IOException, InterruptedException {

        final Configuration configuration = attempt.getConfiguration();
        final Storage storage = StorageProvider.provider(configuration).storage(configuration);
        return new RecordReader<Ref, Instance>() {
            @Override
            public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {

                return false;
            }

            @Override
            public Ref getCurrentKey() throws IOException, InterruptedException {

                return null;
            }

            @Override
            public Instance getCurrentValue() throws IOException, InterruptedException {

                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {

                return 0;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
