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
                final String id = ref.getId();
                transaction = action.apply(transaction, schema, id);
            }

            @Override
            public void close(final TaskAttemptContext context) {

                transaction.write().join();
            }
        };
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException {

        final Configuration configuration = context.getConfiguration();
        final Storage storage = StorageProvider.provider(configuration).storage(configuration);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException {

        final Configuration configuration = context.getConfiguration();
        final Storage storage = StorageProvider.provider(configuration).storage(configuration);
        return new OutputCommitter() {
            @Override
            public void setupJob(final JobContext jobContext) throws IOException {

            }

            @Override
            public void setupTask(final TaskAttemptContext context) throws IOException {


            }

            @Override
            public boolean needsTaskCommit(final TaskAttemptContext context) throws IOException {

                return false;
            }

            @Override
            public void commitTask(final TaskAttemptContext context) throws IOException {

            }

            @Override
            public void abortTask(final TaskAttemptContext context) throws IOException {

            }
        };
    }
}
