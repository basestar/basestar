package io.basestar.api.exception;

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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.basestar.api.APIRequest;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

import java.util.Set;

public class UnsupportedMethodException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 405;

    public static final String CODE = "UnsupportedMethod";

    private final Set<APIRequest.Method> supported;

    public UnsupportedMethodException(final Set<APIRequest.Method> supported) {

        this.supported = ImmutableSet.<APIRequest.Method>builder()
                .add(APIRequest.Method.HEAD).add(APIRequest.Method.OPTIONS)
                .addAll(supported).build();
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .putHeader("Allow", Joiner.on(", ").join(supported))
                .setMessage(getMessage());
    }
}
