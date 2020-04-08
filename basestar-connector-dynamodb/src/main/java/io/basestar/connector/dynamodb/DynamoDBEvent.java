package io.basestar.connector.dynamodb;

/*-
 * #%L
 * basestar-connector-dynamodb
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import lombok.Data;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Can probably do this more elegantly...

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamoDBEvent implements Serializable {

    @JsonProperty("Records")
    private List<Record> records;

    public enum EventName {

        INSERT,
        MODIFY,
        REMOVE
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Record implements Serializable {

        private String eventSourceARN;

        private String eventID;

        private EventName eventName;

        private String eventVersion;

        private String eventSource;

        private String awsRegion;

        @JsonDeserialize(converter = MutableStreamRecordConverter.class)
        private StreamRecord dynamodb;

        @JsonDeserialize(builder = MutableIdentityConverter.class)
        private Identity userIdentity;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class MutableStreamRecord {

        @JsonProperty("ApproximateCreationDateTime")
        private Instant approximateCreationDateTime;

        @JsonProperty("Keys")
        private Map<String, MutableAttributeValue> keys;

        @JsonProperty("NewImage")
        private Map<String, MutableAttributeValue> newImage;

        @JsonProperty("OldImage")
        private Map<String, MutableAttributeValue> oldImage;

        @JsonProperty("SequenceNumber")
        private String sequenceNumber;

        @JsonProperty("SizeBytes")
        private Long sizeBytes;

        @JsonProperty("StreamViewType")
        private String streamViewType;

        public StreamRecord convert() {

            return StreamRecord.builder()
                    .approximateCreationDateTime(approximateCreationDateTime)
                    .keys(MutableAttributeValue.convert(keys))
                    .newImage(MutableAttributeValue.convert(newImage))
                    .oldImage(MutableAttributeValue.convert(oldImage))
                    .sequenceNumber(sequenceNumber)
                    .sizeBytes(sizeBytes)
                    .streamViewType(streamViewType)
                    .build();
        }
    }

    public static class MutableStreamRecordConverter extends StdConverter<MutableStreamRecord, StreamRecord> {

        @Override
        public StreamRecord convert(final MutableStreamRecord v) {

            return v.convert();
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class MutableIdentity {

        @JsonProperty("PrincipalId")
        private String principalId;

        @JsonProperty("Type")
        private String type;

        public Identity convert() {

            return Identity.builder()
                    .principalId(principalId)
                    .type(type)
                    .build();
        }
    }

    public static class MutableIdentityConverter extends StdConverter<MutableIdentity, Identity> {

        @Override
        public Identity convert(final MutableIdentity v) {

            return v.convert();
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class MutableAttributeValue {

        @JsonProperty("S")
        private String s;

        @JsonProperty("N")
        private String n;

        @JsonProperty("B")
        private SdkBytes b;

        @JsonProperty("SS")
        private List<String> ss;

        @JsonProperty("NS")
        private List<String> ns;

        @JsonProperty("BS")
        private List<SdkBytes> bs;

        @JsonProperty("M")
        private Map<String, MutableAttributeValue> m;

        @JsonProperty("L")
        private List<MutableAttributeValue> l;

        @JsonProperty("BOOL")
        private Boolean bool;

        @JsonProperty("NUL")
        private Boolean nul;

        public static Map<String, AttributeValue> convert(final Map<String, MutableAttributeValue> v) {

            if(v == null) {
                return null;
            } else {
                return v.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().convert()));
            }
        }

        public static List<AttributeValue> convert(final List<MutableAttributeValue> v) {

            if(v == null) {
                return null;
            } else {
                return v.stream().map(MutableAttributeValue::convert)
                        .collect(Collectors.toList());
            }
        }

        private AttributeValue convert() {

            return AttributeValue.builder()
                    .s(s).n(n).b(b)
                    .ss(ss).ns(ns).bs(bs)
                    .m(convert(m)).l(convert(l))
                    .bool(bool).nul(nul)
                    .build();
        }
    }
}
