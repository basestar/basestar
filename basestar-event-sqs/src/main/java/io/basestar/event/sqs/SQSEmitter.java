//package io.basestar.event.sqs;

/*-
 * #%L
 * basestar-event-sqs
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
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.collect.Lists;
//import io.basestar.event.Emitter;
//import io.basestar.event.Event;
//import io.basestar.event.EventSerialization;
//import lombok.extern.slf4j.Slf4j;
//import software.amazon.awssdk.services.sqs.SqsAsyncClient;
//import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
//import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Collectors;
//
//@Slf4j
//public class SQSEmitter implements Emitter {
//
//    private static final int MAX_MESSAGES = 10;
//
//    private final SqsAsyncClient client;
//
//    private final String queueUrl;
//
//    public SQSEmitter(final SqsAsyncClient client, final String queueUrl) {
//
//        this.client = client;
//        this.queueUrl = queueUrl;
//    }
//
//    @Override
//    public CompletableFuture<Void> emit(final Collection<? extends Event> events) {
//
//        return CompletableFuture.allOf(Lists.partition(Lists.newArrayList(events), MAX_MESSAGES).stream().map(chunk -> {
//
//            final List<SendMessageBatchRequestEntry> entries = chunk.stream()
//                    .map(event -> {
//                        final String body = EventSerialization.serialize(event);
//                        return SendMessageBatchRequestEntry.builder()
//                                .messageBody(body)
//                                .build();
//                    }).collect(Collectors.toList());
//
//            final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
//                    .queueUrl(queueUrl)
//                    .entries(entries)
//                    .build();
//
//            log.info("Emit SQS events {}", request);
//
//            return client.sendMessageBatch(request).thenApply(v -> null);
//
//        }).toArray(CompletableFuture<?>[]::new));
//    }
//}
