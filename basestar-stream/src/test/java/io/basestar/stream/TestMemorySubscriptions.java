package io.basestar.stream;

class TestMemorySubscriptions extends TestSubscriptions {

    @Override
    protected Subscriptions subscriber() {

        return new MemorySubscriptions();
    }
}
