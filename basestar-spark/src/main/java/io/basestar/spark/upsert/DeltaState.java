package io.basestar.spark.upsert;

import io.basestar.spark.util.Format;
import io.basestar.spark.util.SparkCatalogUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Pair;
import lombok.Data;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class DeltaState {

    public static final String KEY = "delta";

    private final List<Sequence> sequences;

    @JsonCreator
    public DeltaState(@JsonProperty("sequences") final List<Sequence> sequences) {

        this.sequences = Immutable.list(sequences);
    }

    public Stream<URI> locations(final URI location, final List<String> partitionColumns, @Nullable final Set<Map<String, String>> partitionFilter) {

        return sequences.stream().flatMap(sequence -> sequence.locations(location, partitionColumns, partitionFilter));
    }

    public List<CatalogTablePartition> toCatalogPartitions(final URI location, final List<String> partitionColumns) {

        return sequences.stream().flatMap(v -> v.toCatalogPartitions(location, partitionColumns).stream()).collect(Collectors.toList());
    }

    public final DeltaState mergeSequences(final Map<String, Map<UpsertOp, Set<Map<String, String>>>> merge) {

        final List<Sequence> merged = new ArrayList<>();
        final Map<String, Map<UpsertOp, Set<Map<String, String>>>> unprocessed = new TreeMap<>(merge);
        for(final Sequence seq : sequences) {
            final String sequence = seq.getSequence();
            if(unprocessed.containsKey(sequence)) {
                merged.add(seq.merge(unprocessed.get(sequence)));
                unprocessed.remove(sequence);
            } else {
                merged.add(seq);
            }
        }
        unprocessed.forEach((sequence, operations) -> merged.add(new Sequence(sequence, operations)));
        return new DeltaState(merged);
    }

    public DeltaState mergeSequence(final String sequence, final Map<UpsertOp, Set<Map<String, String>>> operations) {

        return mergeSequences(Immutable.map(sequence, operations));
    }

    public DeltaState dropSequences(final Set<String> drop) {

        final List<Sequence> dropped = new ArrayList<>();
        for(final Sequence seq : sequences) {
            if(!drop.contains(seq.getSequence())) {
                dropped.add(seq);
            }
        }
        return new DeltaState(dropped);
    }

    @Data
    public static class Sequence {

        private final String sequence;

        private final Map<UpsertOp, Set<Map<String, String>>> operations;

        @JsonCreator
        public Sequence(@JsonProperty("sequence") final String sequence, @JsonProperty("operations") final Map<UpsertOp, Set<Map<String, String>>> operations) {

            this.sequence = sequence;
            this.operations = Immutable.map(operations);
        }

        public Stream<URI> locations(final URI location, final List<String> partitionColumns, @Nullable final Set<Map<String, String>> partitionFilter) {

            final List<URI> locations = new ArrayList<>();
            for(final Map.Entry<UpsertOp, Set<Map<String, String>>> entry : operations.entrySet()) {
                final UpsertOp op = entry.getKey();
                for(final Map<String, String> spec : entry.getValue()) {
                    if(partitionFilter == null || partitionFilter.isEmpty() || partitionFilter.stream()
                            .anyMatch(filter -> spec.entrySet().containsAll(filter.entrySet()))) {
                        locations.add(partitionLocation(location, partitionColumns, sequence, op, spec));
                    }
                }
            }
            return locations.stream();
        }

        private static URI partitionLocation(final URI location, final List<String> partitionColumns, final String sequence, final UpsertOp op, final Map<String, String> spec) {

            final List<Pair<String, String>> terms = new ArrayList<>();
            terms.add(Pair.of(UpsertTable.SEQUENCE, sequence));
            terms.add(Pair.of(UpsertTable.OPERATION, op.name()));
            partitionColumns.forEach(col -> terms.add(Pair.of(col, spec.get(col))));

            return SparkCatalogUtils.partitionLocation(location, terms);
        }

        public List<CatalogTablePartition> toCatalogPartitions(final URI location, final List<String> partitionColumns) {

            return operations.entrySet().stream().flatMap(entry -> {

                final UpsertOp op = entry.getKey();
                return entry.getValue().stream().map(baseSpec -> {

                    final Map<String, String> spec = new HashMap<>(baseSpec);
                    spec.put(UpsertTable.SEQUENCE, sequence);
                    spec.put(UpsertTable.OPERATION, op.name());

                    final URI partitionLocation = partitionLocation(location, partitionColumns, sequence, op, spec);
                    return SparkCatalogUtils.partition(spec, Format.PARQUET, partitionLocation);
                });

            }).collect(Collectors.toList());
        }

        public Sequence merge(final Map<UpsertOp, Set<Map<String, String>>> merge) {

            final Map<UpsertOp, Set<Map<String, String>>> merged = new HashMap<>(operations);
            merge.forEach((op, specs) -> merged.compute(op, (ignored, current) -> Immutable.addAll(current, specs)));
            return new Sequence(sequence, merged);
        }
    }
}
