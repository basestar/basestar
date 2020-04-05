package io.basestar.storage.elasticsearch;

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
