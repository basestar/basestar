package io.basestar.storage.spark;

/*-
 * #%L
 * basestar-storage-spark
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
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.spark.SparkSchemaUtils;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagedList;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


@Slf4j
public class SparkStorage implements Storage.WithoutWrite {

    private final SparkSession session;

    private final SparkStrategy strategy;

    private final ExecutorService executor;

    private SparkStorage(final Builder builder) {

        this.session = builder.session;
        this.strategy = builder.strategy;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private SparkSession session;

        private SparkStrategy strategy;

        public SparkStorage build() {

            return new SparkStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return CompletableFuture.supplyAsync(() -> {

            Dataset<Row> ds = strategy.objectRead(session, schema);
            ds = ds.filter(ds.col(Reserved.ID).equalTo(id));
            final Row row = ds.first();
            return row == null ? null : SparkSchemaUtils.fromSpark(schema, row);

        }, executor);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return CompletableFuture.supplyAsync(() -> {

            Dataset<Row> ds = strategy.historyRead(session, schema);
            ds = ds.filter(ds.col(Reserved.ID).equalTo(id)
                    .and(ds.col(Reserved.VERSION).equalTo(version)));
            final Row row = ds.first();
            return row == null ? null : SparkSchemaUtils.fromSpark(schema, row);

        }, executor);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return ImmutableList.of((count, paging, stats) -> CompletableFuture.supplyAsync(() -> {

            final Dataset<Row> input = strategy.objectRead(session, schema);
            final Column column = query.visit(new SparkExpressionVisitor(path -> input.col(path.toString())));
            Dataset<Row> ds = input.filter(column);
            if(paging != null) {
                // FIXME:
            }
            ds = ds.limit(count);

            final List<Row> rows = ds.collectAsList();

            final List<Map<String, Object>> items = rows.stream()
                    .map(row -> SparkSchemaUtils.fromSpark(schema, row))
                    .collect(Collectors.toList());

//            final PagingToken nextToken;
//            if(items.isEmpty()) {
//                nextToken = null;
//            } else {
//                nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, items.get(items.size() - 1));
//            }

            return new PagedList<>(items, null);

        }, executor));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    // FIXME: can request more than one at a time here

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
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

        return SparkStorageTraits.INSTANCE;
    }
}
