package io.basestar.event.sqs;

/*-
 * #%L
 * basestar-event-sqs
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

import com.google.common.io.BaseEncoding;
import io.basestar.event.Event;
import io.basestar.event.EventSerialization;
import io.basestar.event.Handler;
import io.basestar.event.Receiver;
import io.basestar.storage.Stash;
import io.basestar.util.Nullsafe;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SQSReceiver implements Receiver {

    public static final String EVENT_ATTRIBUTE = "event";

    public static final String OVERSIZE_ATTRIBUTE = "oversize";

    public static final String ALL_ATTRIBUTES = "All";

    private static final int WAIT_SECONDS = 20;

    private static final int READ_COUNT = 10;

    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    private final SqsAsyncClient client;

    private final String queueUrl;

    private final EventSerialization serialization;

    private final Stash oversizeStash;

    // If true (the default), oversize content will be deleted after the related message is successfully processed.
    // This may not be desired (e.g. when using different queues to process the same messages in different modules).
    private final boolean deleteOversize;

    public SQSReceiver(final Builder builder) {

        this.client = builder.client;
        this.queueUrl = builder.queueUrl;
        this.serialization = Nullsafe.orDefault(builder.serialization, EventSerialization.gzipBson());
        this.oversizeStash = builder.oversizeStash;
        this.deleteOversize = Nullsafe.orDefault(builder.deleteOversize, true);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private SqsAsyncClient client;

        private String queueUrl;

        private EventSerialization serialization;

        private Stash oversizeStash;

        private Boolean deleteOversize;

        public SQSReceiver build() {

            return new SQSReceiver(this);
        }
    }

    @Override
    public CompletableFuture<Integer> receive(final Handler<Event> handler) {

        final ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .waitTimeSeconds(WAIT_SECONDS)
                .maxNumberOfMessages(READ_COUNT)
                .queueUrl(queueUrl)
                .messageAttributeNames(ALL_ATTRIBUTES)
                .build();

        return client.receiveMessage(request)
                .thenCompose(result -> handle(result, handler));
    }

    private CompletableFuture<Integer> handle(final ReceiveMessageResponse result, final Handler<Event> handler) {

        if(result.messages().isEmpty()) {
            return CompletableFuture.completedFuture(0);
        } else {
            return CompletableFuture.allOf(result.messages().stream()
                    .map(m -> this.handle(m, handler))
                    .toArray(CompletableFuture<?>[]::new))
                    .thenApply(ignored -> result.messages().size());
        }
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<?> handle(final Message message, final Handler<Event> handler) {

        final Map<String, MessageAttributeValue> attributes = message.messageAttributes();
        final String eventType = attributes.get(EVENT_ATTRIBUTE).stringValue();
        final Map<String, String> meta = new HashMap<>();
        attributes.forEach((k, v) -> {
            if("String".equals(v.dataType())) {
                meta.put(k, v.stringValue());
            }
        });
        try {
            final Class<? extends Event> eventClass = (Class<? extends Event>) Class.forName(eventType);
            if (Event.class.isAssignableFrom(eventClass)) {
                final MessageAttributeValue oversizeAttr = attributes.get(OVERSIZE_ATTRIBUTE);
                if (oversizeAttr != null) {

                    final String oversize = oversizeAttr.stringValue();
                    return oversizeStash.read(oversize)
                            .thenCompose(bytes -> {
                                final Event event = serialization.deserialize(eventClass, bytes);
                                final CompletableFuture<?> handleFuture = handle(message, event, meta, handler);
                                if(deleteOversize) {
                                    return handleFuture.thenCompose(ignored -> oversizeStash.delete(oversize));
                                } else {
                                    return handleFuture.thenApply(ignored -> null);
                                }
                            });
                } else {

                    final byte[] bytes = BASE_ENCODING.decode(message.body());
                    final Event event = serialization.deserialize(eventClass, bytes);
                    return handle(message, event, meta, handler);
                }

            } else {
                throw new IllegalStateException();
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private CompletableFuture<?> handle(final Message message, final Event event, final Map<String, String> meta, final Handler<Event> handler) {

        log.debug("Handling event {}", message);
        return handler.handle(event, meta)
                .thenCompose(ignored -> client.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build()));
    }
}
