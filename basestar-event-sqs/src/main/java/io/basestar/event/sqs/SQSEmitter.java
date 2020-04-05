//package io.basestar.event.sqs;
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
