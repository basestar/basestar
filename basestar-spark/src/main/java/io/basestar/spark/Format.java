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

import java.util.Arrays;

public enum Format {

    PARQUET {

        @Override
        public String getHadoopInputFormat() {

            return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
        }

        @Override
        public String getHadoopOutputFormat() {

            return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
        }

        @Override
        public String getHadoopSerde() {

            return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        }
    };

    public static final Format DEFAULT = PARQUET;

    public static Format forHadoopSerde(final String serde) {

        return Arrays.stream(values()).filter(v -> serde.equals(v.getHadoopSerde()))
                .findFirst().orElseThrow(IllegalStateException::new);
    }

    public String getSparkFormat() {

        return name().toLowerCase();
    }

    public abstract String getHadoopInputFormat();

    public abstract String getHadoopOutputFormat();

    public abstract String getHadoopSerde();
}
