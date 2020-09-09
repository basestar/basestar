package io.basestar.spark.source;

/*-
 * #%L
 * basestar-spark
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

import io.basestar.spark.sink.Sink;
import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class JDBCSource implements Source<Dataset<Row>> {

    private final SparkSession session;

    private final String jdbcUrl;

    private final String table;

    private final Properties properties;

    private final Map<String, String> options;

    @lombok.Builder(builderClassName = "Builder")
    JDBCSource(final SparkSession session, final String jdbcUrl, final String table, final Properties properties, final Map<String, String> options) {

        this.session = Nullsafe.require(session);
        this.jdbcUrl = Nullsafe.require(jdbcUrl);
        this.table = Nullsafe.require(table);
        this.properties = Nullsafe.orDefault(properties, Properties::new);
        this.options = Nullsafe.orDefault(options);
    }

    @Override
    public void then(final Sink<Dataset<Row>> sink) {

        final Dataset<Row> dataset = session.read()
                .options(options)
                .jdbc(jdbcUrl, table, properties);
        log.info("Loaded {} {}", jdbcUrl, table);
        sink.accept(dataset);
    }
}
