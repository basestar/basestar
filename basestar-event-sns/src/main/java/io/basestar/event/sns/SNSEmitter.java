package io.basestar.event.sns;

/*-
 * #%L
 * basestar-event-sns
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

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.EventSerialization;
import io.basestar.storage.Stash;
import io.basestar.util.Nullsafe;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SNSEmitter implements Emitter {

    private static final String EVENT_ATTRIBUTE = "event";

    private static final String MODULE_ATTRIBUTE = "module";

    private static final String OVERSIZE_ATTRIBUTE = "oversize";

    private static final String DEDUPLICATION_ATTRIBUTE = "MessageDeduplicationId";

    private static final int MAX_SIZE = 256_000;

    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    private final SnsAsyncClient client;

    private final String topicArn;

    private final EventSerialization serialization;

    private final Stash oversizeStash;

    private SNSEmitter(final Builder builder) {

        this.client = builder.client;
        this.topicArn = builder.topicArn;
        this.serialization = Nullsafe.option(builder.serialization, EventSerialization.gzipBson());
        this.oversizeStash = builder.oversizeStash;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private SnsAsyncClient client;

        private String topicArn;

        private Stash oversizeStash;

        private EventSerialization serialization;

        public SNSEmitter build() {

            return new SNSEmitter(this);
        }
    }

    @Override
    public CompletableFuture<?> emit(final Collection<? extends Event> events, final Map<String, String> meta) {

        return CompletableFuture.allOf(events.stream()
                .map(event -> {

                    final String id = UUID.randomUUID().toString();

                    // FIXME: these need to be included in sizing
                    final Map<String, MessageAttributeValue> attributes = new HashMap<>();

                    final String eventType = event.eventType();
                    final String eventModule = event.eventModule();

                    attributes.put(EVENT_ATTRIBUTE, stringAttribute(eventType));
                    attributes.put(MODULE_ATTRIBUTE, stringAttribute(eventModule));
                    attributes.put(DEDUPLICATION_ATTRIBUTE, stringAttribute(id));
                    meta.forEach((k, v) -> attributes.put(k, stringAttribute(v)));

                    final byte[] body = serialization.serialize(event);
                    final String encoded = BASE_ENCODING.encode(body);
                    if(encoded.getBytes(Charsets.UTF_8).length > MAX_SIZE) {
                        return oversizeStash.write(id, body)
                                .thenCompose(ref -> {
                                    attributes.put(OVERSIZE_ATTRIBUTE, stringAttribute(ref));
                                    final byte[] abbreviated = serialization.serialize(event.abbreviate());
                                    return emitImpl(attributes, BASE_ENCODING.encode(abbreviated));
                                });
                    } else {
                        return emitImpl(attributes, encoded);
                    }

                }).toArray(CompletableFuture<?>[]::new));
    }

    private static MessageAttributeValue stringAttribute(final String value) {

        return MessageAttributeValue.builder().dataType("String").stringValue(value).build();
    }

    private CompletableFuture<?> emitImpl(final Map<String, MessageAttributeValue> attributes, final String message) {

        final PublishRequest request = PublishRequest.builder()
                .targetArn(topicArn)
                .messageAttributes(attributes)
                .message(message)
                .build();

        log.debug("Publish to SNS topic {}", request);

        return client.publish(request);
    }
}
