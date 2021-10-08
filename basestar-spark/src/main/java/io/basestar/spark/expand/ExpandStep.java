package io.basestar.spark.expand;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.util.Bucket;
import io.basestar.spark.query.QueryResolver;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface ExpandStep extends Serializable {

    ExpandStep getNext();

    StructType projectKeysType(StructType inputType);

    Column[] projectedKeyColumns();

    Iterator<Row> projectKeys(StructType outputType, Row row);

    Dataset<Row> apply(QueryResolver resolver, Dataset<Row> input, Set<Bucket> buckets);

    static ExpandStep from(final LinkableSchema root, final Set<Expansion> expansions) {

        if (expansions.isEmpty()) {
            return null;
        }

        ExpandStep last = from(root, Expansion.expansion(expansions));

        final Map<ReferableSchema, Set<Name>> refs = new HashMap<>();
        for (final Expansion expansion : expansions) {
            if (expansion instanceof Expansion.OfRef) {
                final Expansion.OfRef ofRef = (Expansion.OfRef) expansion;
                refs.compute(ofRef.getTarget(), (k, v) -> Immutable.add(v, ofRef.getName()));
            } else {
                assert expansion instanceof Expansion.OfLink;
                final Expansion.OfLink ofLink = (Expansion.OfLink) expansion;
                last = new ExpandLinkStep(last, root, ofLink.getSource(), ofLink.getLink(), ofLink.getName());
            }
        }

        for (final Map.Entry<ReferableSchema, Set<Name>> entry : refs.entrySet()) {
            final ReferableSchema target = entry.getKey();
            final Set<Name> names = entry.getValue();
            last = new ExpandRefsStep(last, root, target, names);
        }

        return last;
    }
}
