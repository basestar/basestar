package io.basestar.storage.elasticsearch;

import io.basestar.storage.Metadata;
import lombok.Data;

@Data
public class ElasticsearchMetadata implements Metadata {

    private final long primaryTerm;

    private final long seqNo;
}
