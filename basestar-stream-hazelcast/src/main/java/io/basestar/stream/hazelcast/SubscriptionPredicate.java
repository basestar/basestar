package io.basestar.stream.hazelcast;

import com.hazelcast.query.Predicate;
import io.basestar.stream.Change;
import io.basestar.stream.Subscription;
import io.basestar.stream.SubscriptionKey;
import io.basestar.util.Name;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;

@Data
@Slf4j
public class SubscriptionPredicate implements Serializable, Predicate<SubscriptionKey, Subscription> {

    private final Name schema;

    private final Change.Event event;

    private final Map<String, Object> before;

    private final Map<String, Object> after;

    @Override
    public boolean apply(final Map.Entry<SubscriptionKey, Subscription> entry) {

        try {
            return entry.getValue().matches(schema, event, before, after);
        } catch (final Exception e) {
            log.error("Failed subscription predicate: " + e.getClass() + ": " + e.getMessage());
            return false;
        }
    }
}
