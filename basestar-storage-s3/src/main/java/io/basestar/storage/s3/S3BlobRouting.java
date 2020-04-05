package io.basestar.storage.s3;

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public interface S3BlobRouting {

    String objectBucket(ObjectSchema schema);

    String historyBucket(ObjectSchema schema);

    String objectPrefix(ObjectSchema schema);

    String historyPrefix(ObjectSchema schema);

    @Data
    @RequiredArgsConstructor
    class Simple implements S3BlobRouting {

        private final String bucket;

        private final String prefix;

        @Override
        public String objectBucket(final ObjectSchema schema) {

            return bucket;
        }

        @Override
        public String historyBucket(final ObjectSchema schema) {

            return bucket;
        }

        @Override
        public String objectPrefix(final ObjectSchema schema) {

            return prefix(schema);
        }

        @Override
        public String historyPrefix(final ObjectSchema schema) {

            return prefix(schema);
        }

        protected String prefix(final ObjectSchema schema) {

            final String base = (prefix == null ? "" : (prefix + (prefix.endsWith("/") ? "" : "/")));
            return base + schema.getName() + "/";
        }
    }
}
