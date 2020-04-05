package io.basestar.storage.elasticsearch.mapping;

import lombok.Builder;
import lombok.Data;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

@Data
@Builder
public class Settings {

    private final int shards;

    private final int replicas;

    public XContentBuilder build(XContentBuilder builder) throws IOException {

        builder = builder.field("number_of_shards", shards);
        builder = builder.field("number_of_replicas", replicas);
        return builder;
    }
}
