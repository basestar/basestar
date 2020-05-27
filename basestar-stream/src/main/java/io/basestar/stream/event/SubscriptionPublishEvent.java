package io.basestar.stream.event;

import io.basestar.database.event.ObjectEvent;
import io.basestar.event.Event;
import io.basestar.stream.Change;
import io.basestar.stream.Subscription;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class SubscriptionPublishEvent implements ObjectEvent {

    private String schema;

    private String id;

    private Change.Event event;

    private Long before;

    private Long after;

    private Subscription subscription;

    public static SubscriptionPublishEvent of(final String schema, final String id, final Change.Event event, final Long before, final Long after, final Subscription subscription) {

        return new SubscriptionPublishEvent().setSchema(schema).setId(id).setEvent(event).setBefore(before).setAfter(after).setSubscription(subscription);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}
