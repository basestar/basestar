package io.basestar.connector.sqs;

/*-
 * #%L
 * basestar-connector-sqs
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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.common.io.BaseEncoding;
import io.basestar.event.Event;
import io.basestar.event.EventSerialization;
import io.basestar.event.Handler;
import io.basestar.event.sqs.SQSReceiver;
import io.basestar.storage.Stash;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SQSStreamHandler implements RequestHandler<SQSEvent, Void> {

    private final Handler<Event> handler;

    private final EventSerialization serialization;

    private final Stash oversizeStash;

    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    public SQSStreamHandler(final Handler<Event> handler,
                            final EventSerialization serialization,
                            final Stash oversizeStash) {

        this.handler = handler;
        this.serialization = serialization;
        this.oversizeStash = oversizeStash;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void handleRequest(final SQSEvent sqsEvent, final Context context) {

        try {
            for(final SQSEvent.SQSMessage message : sqsEvent.getRecords()) {

                final Map<String, SQSEvent.MessageAttribute> attributes = message.getMessageAttributes();
                final String eventType = attributes.get(SQSReceiver.EVENT_ATTRIBUTE).getStringValue();
                final Map<String, String> meta = new HashMap<>();
                attributes.forEach((k, v) -> {
                    if("String".equals(v.getDataType())) {
                        meta.put(k, v.getStringValue());
                    }
                });

                final Class<? extends Event> eventClass = (Class<? extends Event>) Class.forName(eventType);
                if (Event.class.isAssignableFrom(eventClass)) {
                    final byte[] bytes;
                    final SQSEvent.MessageAttribute refAttr = attributes.get(SQSReceiver.OVERSIZE_ATTRIBUTE);
                    if (refAttr != null) {
                        bytes = oversizeStash.read(refAttr.getStringValue()).join();
                    } else {
                        bytes = BASE_ENCODING.decode(message.getBody());
                    }

                    final Event event = serialization.deserialize(eventClass, bytes);
                    handler.handle(event, meta).join();
                } else {
                    log.error("Cannot deserialize an event into class {}", eventClass);
                }
            }

            return null;

        } catch(final Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
