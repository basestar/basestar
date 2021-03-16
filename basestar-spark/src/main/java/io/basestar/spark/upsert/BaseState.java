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
public class BaseState {

    public static final String KEY = "base";

    private final List<Partition> partitions;

    @JsonCreator
    public BaseState(@JsonProperty("partitions") final List<Partition> partitions) {

        this.partitions = Immutable.list(partitions);
    }

    public Optional<Partition> partition(final Map<String, String> spec) {

        return partitions.stream().filter(v -> v.getSpec().equals(spec)).findFirst();
    }

    public List<CatalogTablePartition> toCatalogPartitions(final URI location, final List<String> partitionColumns) {

        return partitions.stream().map(v -> v.toCatalogPartition(location, partitionColumns)).collect(Collectors.toList());
    }

    public Stream<Partition> filter(@Nullable final Set<Map<String, String>> partitionFilter) {

        return partitions.stream().filter(part -> partitionFilter == null || partitionFilter.isEmpty() || partitionFilter.stream()
                    .anyMatch(filter -> part.getSpec().entrySet().containsAll(filter.entrySet())));
    }

    public Stream<URI> locations(final URI location, final List<String> partitionColumns, @Nullable final Set<Map<String, String>> partitionFilter) {

        return filter(partitionFilter).map(part -> part.location(location, partitionColumns));
    }

    public BaseState mergePartitions(final Map<Map<String, String>, String> merge) {

        final List<Partition> merged = new ArrayList<>();
        final Map<Map<String, String>, String> unprocessed = new HashMap<>(merge);
        for(final Partition partition : partitions) {
            final String sequence = unprocessed.get(partition.getSpec());
            if(sequence != null) {
                merged.add(partition.withSequence(sequence));
                unprocessed.remove(partition.getSpec());
            } else {
                merged.add(partition);
            }
        }
        unprocessed.forEach((spec, sequence) -> merged.add(new Partition(spec, sequence)));
        return new BaseState(merged);
    }

    @Data
    public static class Partition {

        private final Map<String, String> spec;

        private final String sequence;

        @JsonCreator
        public Partition(@JsonProperty("spec") final Map<String, String> spec, @JsonProperty("sequence") final String sequence) {

            this.spec = Immutable.map(spec);
            this.sequence = sequence;
        }

        public URI location(final URI location, final List<String> partitionColumns) {

            final List<Pair<String, String>> terms = new ArrayList<>();
            partitionColumns.forEach(col -> terms.add(Pair.of(col, spec.get(col))));
            terms.add(Pair.of(UpsertTable.SEQUENCE, sequence));
            return SparkCatalogUtils.partitionLocation(location, terms);
        }

        public CatalogTablePartition toCatalogPartition(final URI location, final List<String> partitionColumns) {

            return SparkCatalogUtils.partition(spec, Format.PARQUET, location(location, partitionColumns));
        }

        public Partition withSequence(final String sequence) {

            return new Partition(spec, sequence);
        }
    }
}
