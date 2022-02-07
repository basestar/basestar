package io.basestar.stream.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.basestar.stream.Subscriptions;
import io.basestar.stream.TestSubscriptions;

public class TestHazelcastSubscriptions extends TestSubscriptions {

    private static HazelcastInstance INSTANCE;

    private static HazelcastInstance instance(final Config config) {

        if (INSTANCE == null) {

            INSTANCE = Hazelcast.newHazelcastInstance(config);
        }
        return INSTANCE;
    }

    @Override
    protected Subscriptions subscriber() {

        final HazelcastInstance instance = instance(new Config());
        return new HazelcastSubscriptions(instance, "subscriptions");
    }
}
