package io.basestar.storage.elasticsearch.mapping;

/*-
 * #%L
 * basestar-storage-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.Map;

@Data
public class Settings {

    private static final int DEFAULT_SHARDS = 6;

    private static final int DEFAULT_REPLICAS = 0;

    private static final String DEFAULT_REFRESH_INTERVAL = "1s";

    private final int shards;

    private final int replicas;

    private final String refreshInterval;

    @lombok.Builder(builderClassName = "Builder", toBuilder = true)
    Settings(final Integer shards, final Integer replicas, final String refreshInterval) {

        this.shards = Nullsafe.orDefault(shards, DEFAULT_SHARDS);
        this.replicas = Nullsafe.orDefault(replicas, DEFAULT_REPLICAS);
        this.refreshInterval = Nullsafe.orDefault(refreshInterval, DEFAULT_REFRESH_INTERVAL);
    }

    public Map<String, ?> dynamic() {

        return ImmutableMap.<String, Object>builder()
                .put("refresh_interval", refreshInterval)
                .build();
    }

    private Map<String, ?> analysis() {

        return ImmutableMap.of(
                "normalizer", normalizer()
        );
    }

    private Map<String, ?> normalizer() {

        return ImmutableMap.of(
                "lowercase", lowercase()
        );
    }

    private Map<String, ?> lowercase() {

        return ImmutableMap.of(
                "type", "custom",
                "filter", ImmutableList.of("lowercase")
        );
    }

    public Map<String, ?> all() {

        return ImmutableMap.<String, Object>builder()
                .putAll(dynamic())
                .put("number_of_shards", shards)
                .put("number_of_replicas", replicas)
                .put("analysis", analysis())
                .build();
    }
}
