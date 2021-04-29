package io.basestar.api;

/*-
 * #%L
 * basestar-api
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.basestar.api.exception.UnsupportedContentException;
import io.basestar.auth.Caller;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface APIRequest {

    Caller getCaller();

    Method getMethod();

    String getPath();

    ListMultimap<String, String> getQuery();

    ListMultimap<String, String> getHeaders();

    InputStream readBody() throws IOException;

    String getRequestId();

    default <T> T readBody(final Class<T> as) throws IOException {

        try(final InputStream is = readBody()) {
            return getContentType().getMapper().readValue(is, as);
        }
    }

    default String getFirstHeader(final String key) {

        for(final Map.Entry<String, String> entry : getHeaders().entries()) {
            if(entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    default String getFirstQuery(final String key) {

        final Collection<String> result = getQuery().get(key);
        return result.isEmpty() ? null : result.iterator().next();
    }

    default APIFormat getContentType() {

        final String format = getFirstQuery("format");
        final String contentType = getFirstHeader("content-type");
        if(format == null && contentType == null) {
            return APIFormat.JSON;
        } else if(format != null) {
            return APIFormat.forFormat(format);
        } else {
            final APIFormat match = APIFormat.bestMatch(contentType);
            if (match == null) {
                throw new UnsupportedContentException(contentType);
            }
            return match;
        }
    }

    default APIFormat getAccept() {

        final String format = getFirstQuery("format");
        final String accept = getFirstHeader("accept");
        if(format == null && accept == null) {
            return getContentType();
        } else if(format != null) {
            return APIFormat.forFormat(format);
        } else {
            final APIFormat match = APIFormat.bestMatch(accept);
            if (match == null) {
                throw new UnsupportedContentException(accept);
            }
            return match;
        }
    }

    static Simple get(final Caller caller, final String path) {

        return request(caller, Method.GET, path, APIFormat.JSON);
    }

    static Simple post(final Caller caller, final String path, final Object body) throws IOException {

        return request(caller, Method.POST, path, APIFormat.JSON, body);
    }

    static Simple put(final Caller caller, final String path, final Object body) throws IOException {

        return request(caller, Method.PUT, path, APIFormat.JSON, body);
    }

    static Simple request(final Caller caller, final Method method, final String path, final APIFormat format) {

        return request(caller, method, path, ImmutableMap.of(), ImmutableMap.of(), format, new byte[0]);
    }

    static Simple request(final Caller caller, final Method method, final String path, final APIFormat format, final Object body) throws IOException {

        return request(caller, method, path, ImmutableMap.of(), ImmutableMap.of(), format, body);
    }

    static Simple request(final Caller caller, final Method method, final String path, final Map<String, String> query, final Map<String, String> headers, final APIFormat format, final Object body) throws IOException {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            if(body != null) {
                format.getMapper().writeValue(baos, body);
            }
            return request(caller, method, path, query, headers, format, baos.toByteArray());
        }
    }

    static Simple request(final Caller caller, final Method method, final String path, final Map<String, String> query, final Map<String, String> headers, final APIFormat format, final byte[] body) {

        final ListMultimap<String, String> allQuery = ArrayListMultimap.create();
        query.forEach(allQuery::put);
        final ListMultimap<String, String> allHeaders = ArrayListMultimap.create();
        headers.forEach(allHeaders::put);
        allHeaders.put("Accept", format.getContentType());
        if(body != null) {
            allHeaders.put("Content-Type", format.getContentType());
        }
        return new Simple(caller, method, path, allQuery, allHeaders, body, UUID.randomUUID().toString());
    }

    @Data
    class Simple implements APIRequest {

        private final Caller caller;

        private final Method method;

        private final String path;

        private final ListMultimap<String, String> query;

        private final ListMultimap<String, String> headers;

        private final byte[] body;

        private final String requestId;

        @Override
        public InputStream readBody() {

            return new ByteArrayInputStream(body);
        }
    }

    @RequiredArgsConstructor
    class Delegating implements APIRequest {

        private final APIRequest delegate;

        @Override
        public Caller getCaller() {

            return delegate.getCaller();
        }

        public Method getMethod() {

            return delegate.getMethod();
        }

        public String getPath() {

            return delegate.getPath();
        }

        public ListMultimap<String, String> getQuery() {

            return delegate.getQuery();
        }

        public ListMultimap<String, String> getHeaders() {

            return delegate.getHeaders();
        }

        public InputStream readBody() throws IOException {

            return delegate.readBody();
        }

        @Override
        public String getRequestId() {

            return delegate.getRequestId();
        }
    }

    enum Method {

        HEAD,
        OPTIONS,
        GET,
        POST,
        PATCH,
        PUT,
        DELETE
    }
}
