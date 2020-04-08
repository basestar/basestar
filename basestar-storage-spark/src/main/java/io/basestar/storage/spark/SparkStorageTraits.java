package io.basestar.storage.spark;

/*-
 * #%L
 * basestar-storage-spark
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

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class SparkStorageTraits implements StorageTraits {

    public static final SparkStorageTraits INSTANCE = new SparkStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.NONE;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.NONE;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        return Consistency.NONE;
    }

    @Override
    public boolean supportsPolymorphism() {

        // FIXME
        return false;
    }

    @Override
    public boolean supportsMultiObject() {

        // FIXME
        return false;
    }

    @Override
    public Concurrency getObjectConcurrency() {

        // FIXME
        return Concurrency.NONE;
    }
}
