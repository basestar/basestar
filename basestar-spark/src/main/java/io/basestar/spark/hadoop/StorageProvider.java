package io.basestar.spark.hadoop;

import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface StorageProvider {

    String PROVIDER = "io.basestar.storage.provider";

    String OUTPUT_CONSISTENCY = "io.basestar.storage.output.consistency";

    String OUTPUT_VERSIONING = "io.basestar.storage.output.versioning";

    String INPUT_QUERY = "io.basestar.storage.input.query";

    String INPUT_SPLITS = "io.basestar.storage.input.splits";

    String SCHEMA = "io.basestar.storage.schema";

    Namespace namespace(Configuration configuration);

    Storage storage(Configuration configuration);

    default Name schema(final Configuration configuration) {

        final String str = configuration.get(SCHEMA);
        if(str == null) {
            throw new IllegalStateException("Schema must be specified using " + SCHEMA);
        } else {
            return Name.parse(str);
        }
    }

    default Consistency outputConsistency(final Configuration configuration) {

        return Nullsafe.mapOrDefault(configuration.get(OUTPUT_CONSISTENCY), Consistency::valueOf, Consistency.NONE);
    }

    default Versioning outputVersioning(final Configuration configuration) {

        return Nullsafe.mapOrDefault(configuration.get(OUTPUT_VERSIONING), Versioning::valueOf, Versioning.UNCHECKED);
    }

    default Expression inputQuery(final Configuration configuration) {

        return Nullsafe.mapOrDefault(configuration.get(INPUT_QUERY), Expression::parse, new Constant(true));
    }

    default int inputSplits(final Configuration configuration) {

        return Nullsafe.mapOrDefault(
                Nullsafe.orDefault(
                        configuration.get(INPUT_SPLITS),
                        configuration.get("mapreduce.map.cpu.vcores")
                ),
                Integer::parseInt, 1);
    }

    static StorageProvider provider(final Configuration configuration) throws IOException {

        final String providerClassName = configuration.get(PROVIDER);
        try {
            return (StorageProvider)Class.forName(providerClassName).newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IOException("Cannot create provider", e);
        }
    }

}
