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
import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
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

        if(exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        exchange.getRequestReceiver().receiveFullBytes(this::handleBody, this::handleException);
    }

    private void handleBody(final HttpServerExchange exchange, final byte[] body) {

        try {

            final APIRequest.Method method = APIRequest.Method.valueOf(exchange.getRequestMethod().toString());
            final String path = exchange.getRelativePath();
            final Multimap<String, String> query = ArrayListMultimap.create();
            exchange.getQueryParameters().forEach(query::putAll);
            final Multimap<String, String> requestHeaders = ArrayListMultimap.create();
            exchange.getRequestHeaders()
                    .forEach(vs -> requestHeaders.putAll(vs.getHeaderName().toString().toLowerCase(), vs));

            log.debug("Handling HTTP request (method:{} path:{} query:{} headers:{})", method, path, query, requestHeaders);

            final APIRequest request = new APIRequest() {

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

                    return requestHeaders;
                }

                @Override
                public InputStream readBody() {

                    return new ByteArrayInputStream(body);
                }
            };

            final APIResponse response = api.handle(request).exceptionally(e -> {
                log.error("Uncaught", e);
                return APIResponse.error(request, e);
            }).get();

            final int responseCode = response.getStatusCode();
            final Multimap<String, String> responseHeaders = response.getHeaders();

            log.debug("Response (method:{} path:{} status: {} headers: {})", method, path, responseCode, responseHeaders);

            final HeaderMap exchangeResponseHeaders = exchange.getResponseHeaders();
            exchange.setStatusCode(responseCode);
            responseHeaders.entries().forEach(e -> exchangeResponseHeaders.put(HttpString.tryFromString(e.getKey()), e.getValue()));

            final ByteBuffer buffer;
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                response.writeTo(baos);
                baos.flush();
                buffer = ByteBuffer.wrap(baos.toByteArray());
            }
            exchange.getResponseSender().send(buffer, new IoCallback() {
                @Override
                public void onComplete(final HttpServerExchange httpServerExchange, final Sender sender) {

                    httpServerExchange.endExchange();
                }

                @Override
                public void onException(final HttpServerExchange httpServerExchange, final Sender sender, final IOException e) {

                    log.warn("Exception caught while writing a response", e);
                    httpServerExchange.endExchange();
                }
            });

        } catch (final InterruptedException | ExecutionException | IOException e) {
            log.warn("Exception caught during request handling", e);
            exchange.endExchange();
            throw new IllegalStateException(e);
        } catch (final Throwable e) {
            log.warn("Runtime exception caught during request handling", e);
            exchange.endExchange();
            throw e;
        }
    }

    private void handleException(final HttpServerExchange exchange, final Exception e) {

        log.error("Exception in HTTP exchange", e);
    }
}
