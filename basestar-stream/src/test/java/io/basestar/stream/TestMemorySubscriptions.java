package io.basestar.stream;

public class TestMemorySubscriptions extends TestSubscriptions {

    @Override
    protected Subscriptions subscriber() {

        return new MemorySubscriptions();
    }
}
