package io.basestar.example;

/*-
 * #%L
 * basestar-example
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestRequests {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static APIRequest get(final String path, final Multimap<String, String> query, final Multimap<String, String> headers) {

        return request(APIRequest.Method.GET, path, query, headers, null);
    }

    public static APIRequest put(final String path, final Multimap<String, String> query, final Multimap<String, String> headers, final Object body) {

        return request(APIRequest.Method.PUT, path, query, headers, body);
    }

    public static APIRequest post(final String path, final Multimap<String, String> query, final Multimap<String, String> headers, final Object body) {

        return request(APIRequest.Method.POST, path, query, headers, body);
    }

    public static APIRequest delete(final String path, final Multimap<String, String> query, final Multimap<String, String> headers) {

        return request(APIRequest.Method.DELETE, path, query, headers);
    }

    public static APIRequest request(final APIRequest.Method method, final String path, final Multimap<String, String> query, final Multimap<String, String> headers) {

        return request(method, path, query, headers, null);
    }

    public static APIRequest request(final APIRequest.Method method, final String path, final Multimap<String, String> query, final Multimap<String, String> headers, final Object body) {

        return new APIRequest() {
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
            public InputStream readBody() throws IOException {

                if(body != null) {
                    return new ByteArrayInputStream(objectMapper.writeValueAsBytes(body));
                } else {
                    return null;
                }
            }
        };
    }

    public static <T> T responseBody(final APIResponse response, final Class<T> cls) throws IOException {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            response.writeTo(baos);
            return objectMapper.readValue(baos.toByteArray(), cls);
        }
    }
}
