package io.basestar.stream.hazelcast;

import com.hazelcast.query.Predicate;
import io.basestar.stream.Subscription;
import io.basestar.stream.SubscriptionKey;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;

@Data
@Slf4j
public class SubscriptionSubPredicate implements Serializable, Predicate<SubscriptionKey, Subscription> {

    private final String sub;

    @Override
    public boolean apply(final Map.Entry<SubscriptionKey, Subscription> entry) {

        try {
            return entry.getValue().matches(sub);
        } catch (final Exception e) {
            log.error("Failed subscription predicate: " + e.getClass() + ": " + e.getMessage());
            return false;
        }
    }
}
