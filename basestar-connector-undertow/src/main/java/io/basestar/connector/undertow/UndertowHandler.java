package io.basestar.connector.undertow;

/*-
 * #%L
 * basestar-connector-undertow
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.basestar.api.API;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import lombok.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

@Data
public class UndertowHandler implements HttpHandler {

    private final API api;

    @Override
    public void handleRequest(final HttpServerExchange exchange) {

        exchange.getRequestReceiver().receiveFullBytes(this::handleBody, this::handleException);
    }

    private void handleBody(final HttpServerExchange exchange, final byte[] body) {

        try {
            final APIResponse response = api.handle(new APIRequest() {

                @Override
                public Method getMethod() {

                    return Method.valueOf(exchange.getRequestMethod().toString());
                }

                @Override
                public String getPath() {

                    return exchange.getRelativePath();
                }

                @Override
                public Multimap<String, String> getQuery() {

                    final Multimap<String, String> result = ArrayListMultimap.create();
                    exchange.getQueryParameters().forEach(result::putAll);
                    return result;
                }

                @Override
                public Multimap<String, String> getHeaders() {

                    final Multimap<String, String> result = ArrayListMultimap.create();
                    exchange.getRequestHeaders()
                            .forEach(vs -> result.putAll(vs.getHeaderName().toString(), vs));
                    return result;
                }

                @Override
                public InputStream readBody() {

                    return new ByteArrayInputStream(body);
                }

            }).get();

            exchange.setStatusCode(response.getStatusCode());
            response.getHeaders()
                    .forEach((k, v) -> exchange.getResponseHeaders().put(HttpString.tryFromString(k), v));

            try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                response.writeTo(baos);
                exchange.getResponseSender().send(ByteBuffer.wrap(baos.toByteArray()));

            }
            exchange.endExchange();

        } catch (final IOException | InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handleException(final HttpServerExchange exchange, final Exception e) {

        e.printStackTrace(System.err);
    }
}
