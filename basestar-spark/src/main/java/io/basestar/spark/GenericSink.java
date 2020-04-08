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

import io.basestar.schema.Reserved;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.md5;

@Data
@RequiredArgsConstructor
public class GenericSink<I> implements Sink<Dataset<I>> {

    public static final String DEFAULT_FORMAT = Format.PARQUET;

    private final String format;

    private final String path;

    private final SaveMode mode;

    private final Map<String, String> options;

    private final List<Prefix> prefixes;

    public GenericSink(final Builder builder) {

        this.format = Nullsafe.of(builder.format, DEFAULT_FORMAT);
        this.path = builder.path;
        this.mode = Nullsafe.of(builder.mode, SaveMode.Overwrite);
        this.options = Nullsafe.immutableCopy(builder.options);
        this.prefixes = Nullsafe.immutableCopy(builder.prefixes);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Data
    @Accessors(chain = true, fluent = true)
    public static class Builder {

        private String format;

        private String path;

        private SaveMode mode;

        private Map<String, String> options;

        private List<Prefix> prefixes;

        public <I> GenericSink<I> build() {

            return new GenericSink<>(this);
        }
    }

    @Data
    public static class Prefix {

        private final String column;

        private final int length;

        public static Prefix of(final String column, final int length) {

            return new Prefix(column, length);
        }
    }

    @Override
    public void accept(final Dataset<I> df) {

        final List<String> parts = new ArrayList<>();

        Dataset<?> out = df;
        for(final Prefix prefix : prefixes) {
            final String col = prefix.getColumn();
            final String part = Reserved.PREFIX + col;
            final int len = prefix.getLength();
            out = out.withColumn(part, md5(df.col(col)).substr(0, len));
            parts.add(part);
        }

        out.write()
                .partitionBy(parts.toArray(new String[0]))
                .options(options)
                .mode(mode)
                .format(format)
                .save(path);
    }
}
