package io.basestar.spark.util;

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

import com.google.common.collect.Sets;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import scala.Option;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class SparkUtils {

    public static CatalogStorageFormat storageFormat(final Format format, final URI location) {

        return CatalogStorageFormat.apply(
                Option.apply(location),
                Option.apply(format.getHadoopInputFormat()),
                Option.apply(format.getHadoopOutputFormat()),
                Option.apply(format.getHadoopSerde()),
                true,
                ScalaUtils.emptyScalaMap()
        );
    }

    public static CatalogTablePartition partition(final Map<String, String> spec, final Format format, final URI location) {

        final long now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        return CatalogTablePartition.apply(ScalaUtils.asScalaMap(spec), storageFormat(format, location), ScalaUtils.emptyScalaMap(), now, now, Option.empty());
    }

    public static void syncTablePartitions(final ExternalCatalog catalog, final String databaseName, final String tableName, final List<CatalogTablePartition> partitions) {

        final Map<scala.collection.immutable.Map<String, String>, CatalogTablePartition> target = ScalaUtils.asJavaStream(catalog.listPartitions(databaseName, tableName, Option.empty()))
                .collect(Collectors.toMap(CatalogTablePartition::spec, v -> v));

        final Map<scala.collection.immutable.Map<String, String>, CatalogTablePartition> source = partitions.stream()
                .collect(Collectors.toMap(CatalogTablePartition::spec, v -> v));

        final List<CatalogTablePartition> create = new ArrayList<>();
        final List<CatalogTablePartition> alter = new ArrayList<>();
        final Set<scala.collection.immutable.Map<String, String>> drop = new HashSet<>();

        for(final scala.collection.immutable.Map<String, String> spec : Sets.union(target.keySet(), source.keySet())) {
            final CatalogTablePartition before = target.get(spec);
            final CatalogTablePartition after = source.get(spec);
            if(after == null) {
                drop.add(spec);
            } else if(before == null) {
                create.add(after);
            } else {
                alter.add(after);
            }
        }

        if(!create.isEmpty()) {
            catalog.createPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(create), false);
        }
        if(!alter.isEmpty()) {
            catalog.alterPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(alter));
        }
        if(!drop.isEmpty()) {
            catalog.dropPartitions(databaseName, tableName, ScalaUtils.asScalaSeq(drop), false, true, false);
        }
    }
}
