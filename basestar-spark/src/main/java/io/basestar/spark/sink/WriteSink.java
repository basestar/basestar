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

import io.basestar.spark.util.Format;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.List;
import java.util.Map;

public class WriteSink<T> implements Sink<Dataset<T>> {

    private final Format format;

    private final String path;

    private final SaveMode mode;

    private final Map<String, String> options;

    private final List<String> partitionBy;

    @lombok.Builder(builderClassName = "Builder")
    WriteSink(final Format format, final String path, final SaveMode mode, final Map<String, String> options, final List<String> partitionBy) {

        this.format = Nullsafe.option(format, Format.DEFAULT);
        this.path = Nullsafe.option(path);
        this.mode = Nullsafe.option(mode, SaveMode.ErrorIfExists);
        this.options = Nullsafe.immutableCopy(options);
        this.partitionBy = Nullsafe.immutableCopy(partitionBy);
    }

    @Override
    public void accept(final Dataset<T> df) {

        DataFrameWriter<T> writer = df.write()
                .options(options)
                .mode(mode)
                .format(format.getSparkFormat());
        if(!partitionBy.isEmpty()) {
            writer = writer.partitionBy(partitionBy.toArray(new String[0]));
        }
        writer.save(path);
    }
}
