package io.basestar.spark.aws.transform;

/*-
 * #%L
 * basestar-spark-aws
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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Reserved;
import io.basestar.spark.aws.DynamoDBSparkSchemaUtils;
import io.basestar.spark.transform.Transform;
import org.apache.spark.rdd.RDD;

import java.util.Map;

/**
 * Convert incoming DynamoDB stream record to either the new image if INSERT/MODIFY or a tombstone record with version
 * set above the version from the old image if REMOVE.
 *
 * This is designed for Basestar formatted DynamoDB objects.
 */

public class DynamoDBStreamTransform implements Transform<RDD<Record>, RDD<Map<String, AttributeValue>>> {

    public static final String SEQUENCE = Reserved.PREFIX + "sequence";

    @Override
    public RDD<Map<String, AttributeValue>> accept(final RDD<Record> input) {

        return input.toJavaRDD().map(record -> {

            final String eventName = record.getEventName();
            final Map<String, AttributeValue> before = record.getDynamodb().getOldImage();
            final Map<String, AttributeValue> after = record.getDynamodb().getNewImage();

            if ("REMOVE".equals(eventName)) {
                return addSequence(record, DynamoDBSparkSchemaUtils.tombstone(before));
            } else {
                return addSequence(record, after);
            }

        }).rdd();
    }

    private Map<String, AttributeValue> addSequence(final Record record, final Map<String, AttributeValue> data) {

        return ImmutableMap.<String, AttributeValue>builder().putAll(data)
                .put(SEQUENCE, new AttributeValue().withS(record.getDynamodb().getSequenceNumber()))
                .build();
    }
}
