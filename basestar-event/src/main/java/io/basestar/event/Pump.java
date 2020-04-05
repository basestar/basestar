package io.basestar.event;

public interface Pump { //extends Registry {

    void start();

    void stop();
    
    void flush();

    static Pump create(final Receiver receiver, final Handler<Event> handler, final int minThreads, final int maxThreads) {

        return new DefaultPump(receiver, handler, minThreads, maxThreads);
    }
}
