package io.basestar.test;

/*-
 * #%L
 * basestar-test
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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.okhttp.OkHttpDockerCmdExecFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * For running docker containers in tests, for a given spec, only one container instance
 * will be run. Instance will NOT be terminated at end of tests, rather it will be re-used
 * for another run. This makes tests faster, and local development a lot smoother.
 *
 * Because instances are re-used, it is important to make sure that different resources
 * are used within each container, e.g. always create queues/tables/etc with new random names.
 */

@Slf4j
public class TestContainers {

    private static final String HASH_LABEL = "hash";

    private static DockerClient CLIENT;

    private static final Object LOCK = new Object();

    private static DockerClient start() {

        synchronized (LOCK) {

            if(CLIENT == null) {
                final DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                        .build();
                CLIENT = DockerClientBuilder.getInstance(config)
                        .withDockerCmdExecFactory(new OkHttpDockerCmdExecFactory())
                        .build();
            }
            return CLIENT;
        }
    }

    private static void close() throws IOException {

        synchronized (LOCK) {

            if(CLIENT != null) {
                CLIENT.close();
                CLIENT = null;
            }
        }
    }

    public static CompletableFuture<?> ensure(final ContainerSpec spec) {

        final DockerClient docker = start();

        log.info("Checking to see if ({}) is running", spec);

        final String hash = spec.getHash();
        final List<Container> running = docker.listContainersCmd()
                .withLabelFilter(ImmutableMap.of(HASH_LABEL, hash))
                .exec();

        final String containerId;

        if(running.isEmpty()) {

            log.info("Creating container ({})", spec);

            // FIXME: deprecated port bindings
            final CreateContainerResponse createResponse = docker.createContainerCmd(spec.getImage())
                    .withEnv(spec.getEnv())
                    .withPortBindings(spec.getPorts().stream()
                            .map(v -> PortBinding.parse(v.toString()))
                            .collect(Collectors.toList()))
                    .withLabels(ImmutableMap.of("hash", hash))
                    .exec();

            containerId = createResponse.getId();

            docker.startContainerCmd(containerId).exec();

        } else {

            containerId = running.iterator().next().getId();

            log.info("Already running ({})", containerId);
        }

        final String logName = spec.getImage();

        final CompletableFuture<?> future = new CompletableFuture<>();

        final Pattern waitFor = spec.getWaitFor();

        docker.logContainerCmd(containerId)
                .withStdOut(true)
                .withFollowStream(true)
                .exec(new ResultCallback<Frame>() {

                    private boolean waiting = waitFor != null;

                    @Override
                    public void close() {

                        log.debug("{}: CLOSE", logName);
                    }

                    @Override
                    public void onStart(final Closeable closeable) {

                        log.debug("{}: START", logName);
                    }

                    @Override
                    public void onNext(final Frame frame) {

                        final String line = new String(frame.getPayload(), Charsets.UTF_8).trim();
                        if(waiting) {
                            log.debug("{}: {}", logName, line);
                            if (waitFor.matcher(line).matches()) {
                                waiting = false;
                                future.complete(null);
                            }
                        } else {
                            log.trace("{}: {}", logName, line);
                        }
                    }

                    @Override
                    public void onError(final Throwable throwable) {

                        log.debug("{}: ERROR", logName, throwable);
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {

                        log.debug("{}: COMPLETE", logName);
                        future.complete(null);
                    }
                });

        if(waitFor == null) {
            future.complete(null);
        }

        return future;
    }
}
