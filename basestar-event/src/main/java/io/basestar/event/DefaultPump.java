package io.basestar.event;

/*-
 * #%L
 * basestar-event
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
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

    private static final int SHUTDOWN_WAIT_SECONDS = 5;

    private final Receiver receiver;

    private final Handler<Event> handler;

    private final int minThreads;

    private final int maxThreads;

    private final ScheduledExecutorService executorService;

    private final Counter total = Metrics.counter("events.pump.total");

    private final Object lock = new Object();

    @SuppressWarnings("all")
    private final Random random = new Random();

    private volatile int count;

    public DefaultPump(final Receiver receiver, final Handler<Event> handler, final int minThreads, final int maxThreads) {

        this.receiver = receiver;
        this.handler = handler;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.executorService = Executors.newScheduledThreadPool(minThreads);
        Metrics.gauge("events.pump.threads", this, t -> t.count);
    }

    @Override
    public void start() {

        for(int i = 0; i != minThreads; ++i) {
            another(INITIAL_DELAY_MILLIS + delay());
        }
    }

    @Override
    public void flush() {

        while(true) {
            final Integer results = receiver.receive(handler).join();
            assert results != null;
            if(results == 0) {
                return;
            }
        }
    }

    private void another() {

        another(delay());
    }

    private void another(final long delay) {

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

    private long delay() {

        return (long)MIN_DELAY_MILLIS + (long)random.nextInt(MAX_DELAY_MILLIS - MIN_DELAY_MILLIS);
    }

    @Override
    public void stop() {

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("Failed to shut down executor service");
                }
            }
        } catch (final InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
