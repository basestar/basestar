package io.basestar.event;

/*-
 * #%L
 * basestar-event
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultPump implements Pump {

    private static final int INITIAL_DELAY_MILLIS = 500;

    private static final int MIN_DELAY_MILLIS = 1;

    private static final int MAX_DELAY_MILLIS = 500;

    private final Receiver receiver;

    private final Handler<Event> handler;

    private final int minThreads;

    private final int maxThreads;

    private final ScheduledExecutorService executorService;

    private final Counter total = Metrics.counter("events.pump.total");

    private final Object lock = new Object();

    private final Random random = new Random();

    private volatile int count;

//    private final Multimap<Class<?>, Handler<?>> registry = HashMultimap.create();

    public DefaultPump(final Receiver receiver, final Handler<Event> handler, final int minThreads, final int maxThreads) {

        this.receiver = receiver;
        this.handler = handler;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.executorService = Executors.newScheduledThreadPool(minThreads);
        Metrics.gauge("events.pump.threads", this, t -> t.count);
    }

//    @Override
//    public <E extends Event> void subscribe(final Class<E> event, final Handler<E> handler) {
//
//        registry.put(event, handler);
//    }

    @Override
    public void start() {

        for(int i = 0; i != minThreads; ++i) {
            another(INITIAL_DELAY_MILLIS + delay());
        }
    }

    @Override
    public void flush() {

    }

    private void another() {

        another(delay());
    }

    private void another(long delay) {

        synchronized (lock) {
            if (count < maxThreads) {
                ++count;
                executorService.schedule(() -> {
                    while (true) {
                        try {
                            final Integer results = receiver.receive(handler).join();
                            assert results != null;
                            total.increment(results);
                            synchronized (lock) {
                                if (Thread.interrupted()) {
                                    --count;
                                    return;
                                }
                                if (results == 0) {
                                    if (count > minThreads) {
                                        --count;
                                        return;
                                    }
                                } else {
                                    another();
                                }
                            }
                        } catch (final Throwable e) {
                            log.error("Uncaught in event pump", e);
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

//    @SuppressWarnings("unchecked")
//    private CompletableFuture<?> receive(final Event event) {
//
////        final List<CompletableFuture<?>> futures = new ArrayList<>();
////        for(final Handler<?> handler : registry.get(event.getClass())) {
////            futures.add(((Handler<Event>) handler).handle(event));
////        }
////        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
//    }

    private long delay() {

        return MIN_DELAY_MILLIS + random.nextInt(MAX_DELAY_MILLIS - MIN_DELAY_MILLIS);
    }

    @Override
    public void stop() {

    }
}
