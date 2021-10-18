package io.basestar.stream.hazelcast;

import com.hazelcast.query.Predicate;
import io.basestar.stream.Subscription;
import io.basestar.stream.SubscriptionKey;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class SubscriptionSubPredicate implements Serializable, Predicate<SubscriptionKey, Subscription> {

    private final String sub;

    @Override
    public boolean apply(final Map.Entry<SubscriptionKey, Subscription> entry) {

        return entry.getValue().matches(sub);
    }
}
