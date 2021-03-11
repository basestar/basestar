package io.basestar.spark.hadoop;

import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.util.Ref;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class StorageOutputFormat extends OutputFormat<Ref, WriteAction> {

    @Override
    public RecordWriter<Ref, WriteAction> getRecordWriter(final TaskAttemptContext context) throws IOException {

        final Configuration configuration = context.getConfiguration();
        final StorageProvider provider = StorageProvider.provider(configuration);
        final Storage storage = provider.storage(configuration);
        final Namespace namespace = provider.namespace(configuration);
        final Consistency consistency = provider.outputConsistency(configuration);
        final Versioning versioning = provider.outputVersioning(configuration);
        return new RecordWriter<Ref, WriteAction>() {

            Storage.WriteTransaction transaction = storage.write(consistency, versioning);

            @Override
            public void write(final Ref ref, final WriteAction action) {

                final ObjectSchema schema = namespace.requireObjectSchema(ref.getSchema());
                transaction = action.apply(transaction, schema);
            }

            @Override
            public void close(final TaskAttemptContext context) {

                transaction.write().join();
                provider.close();
            }
        };
    }

    @Override
    public void checkOutputSpecs(final JobContext context) {

        // no action
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {

        return new OutputCommitter() {
            @Override
            public void setupJob(final JobContext jobContext) {

                // no action
            }

            @Override
            public void setupTask(final TaskAttemptContext context) {

                // no action
            }

            @Override
            public boolean needsTaskCommit(final TaskAttemptContext context) {

                return false;
            }

            @Override
            public void commitTask(final TaskAttemptContext context) {

                // no action
            }

            @Override
            public void abortTask(final TaskAttemptContext context) {

                // no action
            }
        };
    }
}
