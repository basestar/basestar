package io.basestar.spark;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.Property;
import io.basestar.schema.Transient;
import io.basestar.util.Path;
import lombok.Builder;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Builder(builderClassName = "Builder")
public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    @NonNull
    private final Function<String, Dataset<Row>> sources;

    @NonNull
    private final InstanceSchema schema;

    @NonNull
    private final Set<Path> expand;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Map<String, Set<Path>> branches = Path.branch(expand);
        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
            final Set<Path> branch = branches.get(entry.getKey());

        }
        if(schema instanceof Link.Resolver) {
            for(final Map.Entry<String, Link> entry : ((Link.Resolver)schema).getLinks().entrySet()) {
                final Set<Path> branch = branches.get(entry.getKey());
                if(branch != null) {

                }
            }
        }
        if(schema instanceof Transient.Resolver) {
            for (final Map.Entry<String, Transient> entry : ((Transient.Resolver) schema).getTransients().entrySet()) {
                final Set<Path> branch = branches.get(entry.getKey());
                if(branch != null) {

                }
            }
        }
        return null;
    }
}
