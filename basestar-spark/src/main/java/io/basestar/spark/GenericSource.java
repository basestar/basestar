package io.basestar.spark;

/*-
 * #%L
 * basestar-spark
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

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.Map;

@Data
@RequiredArgsConstructor
public class GenericSource implements Source<Dataset<Row>> {

    public static final String DEFAULT_FORMAT = Format.PARQUET;

    private final SparkSession session;

    private final String format;

    private final String path;

    private final Map<String, String> options;

    public GenericSource(final SparkSession session, final String path) {

        this(session, DEFAULT_FORMAT, path);
    }

    public GenericSource(final SparkSession session, final String format, final String path) {

        this(session, format, path, Collections.emptyMap());
    }

    @Override
    public void sink(final Sink<Dataset<Row>> sink) {

        sink.accept(session.read()
                .options(options)
                .format(format)
                .load(path));
    }
}
