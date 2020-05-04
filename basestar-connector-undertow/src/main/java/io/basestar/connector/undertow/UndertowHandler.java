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
import io.basestar.auth.Caller;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

@Data
@Slf4j
public class UndertowHandler implements HttpHandler {

    private final API api;

    @Override
    public void handleRequest(final HttpServerExchange exchange) {

        exchange.getRequestReceiver().receiveFullBytes(this::handleBody, this::handleException);
    }

    private void handleBody(final HttpServerExchange exchange, final byte[] body) {

        try {

            final APIRequest.Method method = APIRequest.Method.valueOf(exchange.getRequestMethod().toString());
            final String path = exchange.getRelativePath();
            final Multimap<String, String> query = ArrayListMultimap.create();
            exchange.getQueryParameters().forEach(query::putAll);
            final Multimap<String, String> headers = ArrayListMultimap.create();
            exchange.getRequestHeaders()
                    .forEach(vs -> headers.putAll(vs.getHeaderName().toString(), vs));

            final APIResponse response = api.handle(new APIRequest() {

                @Override
                public Caller getCaller() {

                    return Caller.ANON;
                }

                @Override
                public Method getMethod() {

                    return method;
                }

                @Override
                public String getPath() {

                    return path;
                }

                @Override
                public Multimap<String, String> getQuery() {

                    return query;
                }

                @Override
                public Multimap<String, String> getHeaders() {

                    return headers;
                }

                @Override
                public InputStream readBody() {

                    return new ByteArrayInputStream(body);
                }

            }).get();

            log.info("Handling HTTP request (method:{} path:{} query:{} headers:{})", method, path, query, headers);

            exchange.setStatusCode(response.getStatusCode());
            response.getHeaders()
                    .forEach((k, v) -> exchange.getResponseHeaders().put(HttpString.tryFromString(k), v));

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                response.writeTo(baos);
                exchange.getResponseSender().send(ByteBuffer.wrap(baos.toByteArray()));

            }
            exchange.endExchange();

        } catch (final InterruptedException e) {
            log.warn("Interrupted during request handling", e);
            Thread.currentThread().interrupt();
        } catch (final IOException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handleException(final HttpServerExchange exchange, final Exception e) {

        e.printStackTrace(System.err);
    }
}
