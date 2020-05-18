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

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class ElasticsearchStorageTraits implements StorageTraits {

    public static final ElasticsearchStorageTraits INSTANCE = new ElasticsearchStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.ASYNC;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.ATOMIC;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        // FIXME
        return Consistency.EVENTUAL;
    }

    @Override
    public boolean supportsPolymorphism() {

        return false;
    }

    @Override
    public boolean supportsMultiObject() {

        return false;
    }

    @Override
    public boolean supportsAggregation() {

        // FIXME:
        return false;
    }

    @Override
    public Concurrency getObjectConcurrency() {

        return Concurrency.OPTIMISTIC;
    }
}
