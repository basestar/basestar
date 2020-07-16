package io.basestar.spark.sink;

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

import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class JDBCSink implements Sink<Dataset<?>> {

    private final SparkSession session;

    private final String jdbcUrl;

    private final String table;

    private final SaveMode mode;

    private final Properties properties;

    private final Map<String, String> options;

    @lombok.Builder(builderClassName = "Builder")
    JDBCSink(final SparkSession session, final String jdbcUrl, final String table, final SaveMode mode, final Properties properties, final Map<String, String> options) {

        this.session = Nullsafe.require(session);
        this.jdbcUrl = Nullsafe.require(jdbcUrl);
        this.table = Nullsafe.require(table);
        this.mode = Nullsafe.option(mode, SaveMode.ErrorIfExists);
        this.properties = Nullsafe.option(properties, Properties::new);
        this.options = Nullsafe.option(options);
    }

    @Override
    public void accept(final Dataset<?> input) {

        input.write()
                .options(options)
                .mode(mode)
                .jdbc(jdbcUrl, table, properties);
    }
}
