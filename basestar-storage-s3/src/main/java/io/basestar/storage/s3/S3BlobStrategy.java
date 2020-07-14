package io.basestar.storage.s3;

/*-
 * #%L
 * basestar-storage-s3
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

import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public interface S3BlobStrategy {

    String objectBucket(ObjectSchema schema);

    String historyBucket(ObjectSchema schema);

    String objectPrefix(ObjectSchema schema);

    String historyPrefix(ObjectSchema schema);

    @Data
    @RequiredArgsConstructor
    class Simple implements S3BlobStrategy {

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
            return base + schema.getQualifiedName() + "/";
        }
    }
}
