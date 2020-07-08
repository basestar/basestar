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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public interface APIResponse {

    int getStatusCode();

    Multimap<String, String> getHeaders();

    void writeTo(OutputStream out) throws IOException;

    static APIResponse success(final APIRequest request) {

        return success(request, null);
    }

    static APIResponse success(final APIRequest request, final Object data) {

        return success(request, null, data);
    }

    static APIResponse success(final APIRequest request, final Multimap<String, String> extraHeaders,
                               final Object data) {

        if(data == null) {
            return response(request, 204, extraHeaders, null);
        } else {
            return response(request, 200, extraHeaders, data);
        }
    }

    static APIResponse error(final APIRequest request, final Throwable e) {

        e.printStackTrace(System.err);
        final ExceptionMetadata metadata = exceptionMetadata(e);
        return error(request, metadata);
    }

    static APIResponse error(final APIRequest request, final ExceptionMetadata e) {

        final Map<String, Object> data = new HashMap<>();
        data.put("code", e.getCode());
        data.put("message", e.getMessage());
        data.putAll(e.getData());
        return response(request, e.getStatus(), e.getHeaders(), data);
    }

    static ExceptionMetadata exceptionMetadata(final Throwable e) {

        if(e instanceof HasExceptionMetadata) {
            return ((HasExceptionMetadata)e).getMetadata();
        } else if(e.getCause() != null) {
            return exceptionMetadata(e.getCause());
        } else {
            return new ExceptionMetadata().setStatus(500).setCode("UnknownError").setMessage(e.getMessage());
        }
    }

    static APIResponse response(final APIRequest request, final int status, final Object data) {

        return response(request, status, null, data);
    }

    static APIResponse response(final APIRequest request, final int status,
                                final Multimap<String, String> extraHeaders, final Object data) {

        final APIFormat format = request.getAccept();
        final Multimap<String, String> headers = HashMultimap.create();
        headers.put("Content-Type", format.getContentType());
        final String origin = request.getFirstHeader("Origin");
        if(origin != null) {
            headers.put("Access-Control-Allow-Origin", origin);
        } else {
            headers.put("Access-Control-Allow-Origin", "*");
        }
        final Multimap<String, String> requestHeaders = request.getHeaders();
        final Collection<String> allowMethods = requestHeaders.get("access-control-request-method");
        if(!allowMethods.isEmpty()) {
            headers.put("Access-Control-Allow-Methods", String.join(",", allowMethods));
        } else {
            headers.put("Access-Control-Allow-Methods", "*");
        }
        final Collection<String> allowHeaders = requestHeaders.get("access-control-request-headers");
        if(!allowHeaders.isEmpty()) {
            headers.put("Access-Control-Allow-Headers", String.join(",", allowHeaders));
        } else {
            headers.put("Access-Control-Allow-Headers", "*");
        }
        headers.put("Access-Control-Allow-Credentials", "true");
        if(extraHeaders != null) {
            headers.putAll(extraHeaders);
        }

        return new APIResponse() {
            @Override
            public int getStatusCode() {

                return status;
            }

            @Override
            public Multimap<String, String> getHeaders() {

                return headers;
            }

            @Override
            public void writeTo(final OutputStream out) throws IOException {

                if(data != null) {
                    format.getMapper().writerWithDefaultPrettyPrinter().writeValue(out, data);
                }
            }
        };
    }
}
