package io.basestar.storage.elasticsearch;

/*-
 * #%L
 * basestar-storage-elasticsearch
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import io.basestar.schema.ObjectSchema;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.util.Nullsafe;
import lombok.Builder;

public interface ElasticsearchRouting {

    String objectIndex(ObjectSchema schema);

    String historyIndex(ObjectSchema schema);

    Mappings mappings(ObjectSchema schema);

    Settings settings(ObjectSchema schema);

    @Builder
    class Simple implements ElasticsearchRouting {

        private final String objectPrefix;

        private final String objectSuffix;

        private final String historyPrefix;

        private final String historySuffix;

        private final Settings settings;

        private final Mappings.Factory mappingsFactory;

        @Override
        public String objectIndex(final ObjectSchema schema) {

            return Nullsafe.of(objectPrefix) + schema.getName().toLowerCase() +  Nullsafe.of(objectSuffix);
        }

        @Override
        public String historyIndex(final ObjectSchema schema) {

            return Nullsafe.of(historyPrefix) + schema.getName().toLowerCase() + Nullsafe.of(historySuffix);
        }

        @Override
        public Mappings mappings(final ObjectSchema schema) {

            return mappingsFactory.mappings(schema);
        }

        @Override
        public Settings settings(final ObjectSchema schema) {

            return settings;
        }
//
//        @Override
//        public Mode mode(final ObjectSchema schema) {
//
//            return mode;
//        }
    }
}
