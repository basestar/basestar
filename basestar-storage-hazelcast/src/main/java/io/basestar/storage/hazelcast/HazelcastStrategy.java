package io.basestar.storage.hazelcast;

/*-
 * #%L
 * basestar-storage-hazelcast
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.util.Nullsafe;
import lombok.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface HazelcastStrategy {

    String objectMapName(ReferableSchema schema);

    String historyMapName(ReferableSchema schema);

    default Map<String,MapConfig> mapConfigs(final Namespace namespace) {

        final Map<String, MapConfig> results = new HashMap<>();
        namespace.getSchemas().forEach((name, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                final String historyName = historyMapName(objectSchema);
                final String objectName = objectMapName(objectSchema);
                results.put(historyName, new MapConfig()
                        .setName(historyName));
                results.put(objectName, new MapConfig()
                        .setName(objectName)
                        .setIndexConfigs(indexes(objectSchema)));

            }
        });
        return results;
    }

    static List<IndexConfig> indexes(final ReferableSchema schema) {

        return schema.getIndexes().values().stream()
                .flatMap(index -> {
                    if(!index.isMultiValue()) {
                        final List<String> keys = new ArrayList<>();
                        index.getPartition().forEach(p -> keys.add(p.toString()));
                        index.getSort().forEach(s -> keys.add(s.getName().toString()));

                        return Stream.of(new IndexConfig(IndexType.SORTED, keys.toArray(new String[0])));
                    } else {
                        return Stream.empty();
                    }
                }).collect(Collectors.toList());
    }

    @Builder
    class Simple implements HazelcastStrategy {

        private final String objectPrefix;

        private final String objectSuffix;

        private final String historyPrefix;

        private final String historySuffix;

        @Override
        public String objectMapName(final ReferableSchema schema) {

            return Nullsafe.orDefault(objectPrefix) + schema.getQualifiedName() + Nullsafe.orDefault(objectSuffix);
        }

        @Override
        public String historyMapName(final ReferableSchema schema) {

            return Nullsafe.orDefault(historyPrefix) + schema.getQualifiedName() + Nullsafe.orDefault(historySuffix);
        }

    }
}
