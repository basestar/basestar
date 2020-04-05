package io.basestar.connector.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.common.io.BaseEncoding;
import io.basestar.event.Event;
import io.basestar.event.EventSerialization;
import io.basestar.event.Handler;
import io.basestar.event.sqs.SQSReceiver;
import io.basestar.storage.Stash;

import java.util.Map;

public class SQSStreamHandler implements RequestHandler<SQSEvent, Void> {

    private final Handler<Event> handler;

    private final EventSerialization serialization;

    private final Stash oversizeStash;

    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    public SQSStreamHandler(final Handler<Event> handler,
                            final EventSerialization serialization,
                            final Stash oversizeStash) {

        this.handler = handler;
        this.serialization = serialization;
        this.oversizeStash = oversizeStash;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void handleRequest(final SQSEvent sqsEvent, final Context context) {

        try {
            for(final SQSEvent.SQSMessage message : sqsEvent.getRecords()) {

                final Map<String, SQSEvent.MessageAttribute> attributes = message.getMessageAttributes();
                final String eventType = attributes.get(SQSReceiver.EVENT_ATTRIBUTE).getStringValue();

                final Class<? extends Event> eventClass = (Class<? extends Event>) Class.forName(eventType);
                if (Event.class.isAssignableFrom(eventClass)) {
                    final byte[] bytes;
                    final SQSEvent.MessageAttribute refAttr = attributes.get(SQSReceiver.OVERSIZE_ATTRIBUTE);
                    if (refAttr != null) {
                        bytes = oversizeStash.read(refAttr.getStringValue()).join();
                    } else {
                        bytes = BASE_ENCODING.decode(message.getBody());
                    }

                    final Event event = serialization.deserialize(eventClass, bytes);
                    handler.handle(event).join();
                }
            }

            return null;

        } catch(final Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
