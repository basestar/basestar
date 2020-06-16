package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
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

import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.aggregate.Aggregate;
import io.basestar.storage.util.Pager;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class URLStorage implements Storage {

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        throw new UnsupportedOperationException();
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        throw new UnsupportedOperationException();
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        throw new UnsupportedOperationException();
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        throw new UnsupportedOperationException();
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.SUPPRESS;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return null;
    }
}
