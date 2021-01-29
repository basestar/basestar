package io.basestar.storage.elasticsearch;

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

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.storage.elasticsearch.mapping.Mappings;
import io.basestar.storage.elasticsearch.mapping.Settings;
import io.basestar.util.Nullsafe;
import lombok.Builder;

public interface ElasticsearchStrategy {

    String objectIndex(ReferableSchema schema);

    String historyIndex(ReferableSchema schema);

    Mappings mappings(LinkableSchema schema);

    Settings settings(ReferableSchema schema);

    boolean historyEnabled(ReferableSchema schema);

    @Builder
    class Simple implements ElasticsearchStrategy {

        private final String objectPrefix;

        private final String objectSuffix;

        private final String historyPrefix;

        private final String historySuffix;

        private final Settings settings;

        private final Mappings.Factory mappingsFactory;

        private final boolean historyEnabled;

        private String name(final ReferableSchema schema) {

            return schema.getQualifiedName().toString(Reserved.PREFIX).toLowerCase();
        }

        @Override
        public String objectIndex(final ReferableSchema schema) {

            return Nullsafe.orDefault(objectPrefix) + name(schema) +  Nullsafe.orDefault(objectSuffix);
        }

        @Override
        public String historyIndex(final ReferableSchema schema) {

            return Nullsafe.orDefault(historyPrefix) + name(schema) + Nullsafe.orDefault(historySuffix);
        }

        @Override
        public Mappings mappings(final LinkableSchema schema) {

            return mappingsFactory.mappings(schema);
        }

        @Override
        public Settings settings(final ReferableSchema schema) {

            return settings;
        }

        @Override
        public boolean historyEnabled(final ReferableSchema schema) {

            return historyEnabled;
        }
    }
}
