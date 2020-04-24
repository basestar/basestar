package io.basestar.spark;

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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

@Slf4j
public class ReadSource implements Source<Dataset<Row>> {

    private final SparkSession session;

    private final Format format;

    private final String path;

    private final Map<String, String> options;

    @lombok.Builder(builderClassName = "Builder")
    ReadSource(final SparkSession session, final Format format, final String path, final Map<String, String> options) {

        this.session = Nullsafe.require(session);
        this.format = Nullsafe.option(format, Format.DEFAULT);
        this.path = Nullsafe.option(path);
        this.options = Nullsafe.immutableCopy(options);
    }

    @Override
    public void then(final Sink<Dataset<Row>> sink) {

        try {
            final Dataset<Row> dataset = session.read()
                    .options(options)
                    .format(format.getSparkFormat())
                    .load(path);
            log.info("Loaded {}", path);
            sink.accept(dataset);
        } catch (final Exception e) {
            checkExcept(e);
        }
    }

    @SneakyThrows
    private void checkExcept(final Exception e) {

        if(e instanceof AnalysisException) {
            if(e.getMessage().startsWith("Path does not exist")) {
                return;
            }
        }
        throw e;
    }
}
