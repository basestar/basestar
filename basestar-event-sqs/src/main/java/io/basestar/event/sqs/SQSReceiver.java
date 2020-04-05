package io.basestar.event.sqs;

import com.google.common.base.MoreObjects;
import com.google.common.io.BaseEncoding;
import io.basestar.event.Event;
import io.basestar.event.EventSerialization;
import io.basestar.event.Handler;
import io.basestar.event.Receiver;
import io.basestar.storage.Stash;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SQSReceiver implements Receiver {

    public static final String EVENT_ATTRIBUTE = "event";

    public static final String OVERSIZE_ATTRIBUTE = "oversize";

    private static final int WAIT_SECONDS = 20;

    private static final int READ_COUNT = 10;

    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    private final SqsAsyncClient client;

    private final String queueUrl;

    private final EventSerialization serialization;

    private final Stash oversizeStash;

    public SQSReceiver(final Builder builder) {

        this.client = builder.client;
        this.queueUrl = builder.queueUrl;
        this.serialization = MoreObjects.firstNonNull(builder.serialization, EventSerialization.gzipBson());
        this.oversizeStash = builder.oversizeStash;
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
//                .attributeNames(QueueAttributeName.ALL)
                .messageAttributeNames(EVENT_ATTRIBUTE, OVERSIZE_ATTRIBUTE)
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
        try {
            final Class<? extends Event> eventClass = (Class<? extends Event>) Class.forName(eventType);
            if (Event.class.isAssignableFrom(eventClass)) {
                final MessageAttributeValue refAttr = attributes.get(OVERSIZE_ATTRIBUTE);
                if (refAttr != null) {

                    final String ref = refAttr.stringValue();
                    return oversizeStash.read(ref)
                            .thenCompose(bytes -> {
                                final Event event = serialization.deserialize(eventClass, bytes);
                                return handle(message, event, handler)
                                        .thenCompose(ignored -> oversizeStash.delete(ref));
                            });
                } else {

                    final byte[] bytes = BASE_ENCODING.decode(message.body());
                    final Event event = serialization.deserialize(eventClass, bytes);
                    return handle(message, event, handler);
                }

            } else {
                throw new IllegalStateException();
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private CompletableFuture<?> handle(final Message message, final Event event, final Handler<Event> handler) {

        return handler.handle(event)
                .thenCompose(ignored -> client.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build()));
    }
}
