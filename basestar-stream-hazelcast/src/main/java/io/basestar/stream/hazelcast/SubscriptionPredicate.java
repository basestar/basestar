package io.basestar.stream.hazelcast;

import com.hazelcast.query.Predicate;
import io.basestar.stream.Change;
import io.basestar.stream.Subscription;
import io.basestar.stream.SubscriptionKey;
import io.basestar.util.Name;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class SubscriptionPredicate implements Serializable, Predicate<SubscriptionKey, Subscription> {

    private final Name schema;

    private final Change.Event event;

    private final Map<String, Object> before;

    private final Map<String, Object> after;

    @Override
    public boolean apply(final Map.Entry<SubscriptionKey, Subscription> entry) {

        return entry.getValue().matches(schema, event, before, after);
    }
}
