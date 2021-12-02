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

    private static final int LOG_STATS_MILLIS = 60 * 1000;

    private final String name;

    private final Receiver receiver;

    private final Handler<Event> handler;

    private final int minThreads;

    private final int maxThreads;

    private final ScheduledExecutorService executorService;

    private final Counter total;

    private final Object lock = new Object();

    @SuppressWarnings("all")
    private final Random random = new Random();

    private volatile int count;

    private volatile boolean shutdown;

    public DefaultPump(final String name, final Receiver receiver, final Handler<Event> handler, final int minThreads, final int maxThreads) {

        this.name = name;
        this.receiver = receiver;
        this.handler = handler;
        this.minThreads = minThreads;
        if (this.minThreads < 1) {
            throw new IllegalArgumentException("Min threads must be >= 1");
        }
        this.maxThreads = maxThreads;
        this.executorService = Executors.newScheduledThreadPool(minThreads + 1);
        this.total = Metrics.counter("events.pump.total." + name);
        Metrics.gauge("events.pump.threads." + name, this, t -> t.count);
    }

    @Override
    public void start() {

        for (int i = 0; i != minThreads; ++i) {
            another(INITIAL_DELAY_MILLIS + delay());
        }
        executorService.scheduleWithFixedDelay(this::logInfo, LOG_STATS_MILLIS, LOG_STATS_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush() {

        while (true) {
            final Integer results = receiver.receive(handler).join();
            assert results != null;
            if (results == 0) {
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
                    log.info("Starting event pump thread");
                    try {
                        while (true) {
                            try {
                                final Integer results = receiver.receive(handler).join();
                                assert results != null;
                                total.increment(results);
                                boolean another = false;
                                synchronized (lock) {
                                    if (Thread.interrupted()) {
                                        if (shutdown) {
                                            log.error("Pump interrupted, exiting thread");
                                            --count;
                                            return;
                                        }
                                    }
                                    if (results == 0) {
                                        if (count > minThreads) {
                                            --count;
                                            return;
                                        }
                                    } else {
                                        another = true;
                                    }
                                }
                                if (another) {
                                    another();
                                }
                            } catch (final Throwable e) {
                                log.error("Uncaught in event pump: " + e.getClass() + ": " + e.getMessage(), e);
                            }
                        }
                    } finally {
                        log.info("Leaving event pump thread");
                    }
                }, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void logInfo() {

        log.info("Pump {} stats: (thread count: {}, total events: {})", name, count, total.count());
    }

    private long delay() {

        return (long) MIN_DELAY_MILLIS + (long) random.nextInt(MAX_DELAY_MILLIS - MIN_DELAY_MILLIS);
    }

    @Override
    public void stop() {

        shutdown = true;
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
