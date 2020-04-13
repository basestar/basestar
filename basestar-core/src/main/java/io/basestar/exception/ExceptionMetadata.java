package io.basestar.exception;

/*-
 * #%L
 * basestar-core
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

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Data
@Accessors(chain = true)
public class ExceptionMetadata {

    private int status;

    private String code;

    private String message;

    private final Map<String, Object> data = new HashMap<>();

    public ExceptionMetadata putData(final String key, final Object value) {

        data.put(key, value);
        return this;
    }

    public Map<String, Object> getData() {

        return data;
    }

    public static ExceptionMetadata notFound() {

        return new ExceptionMetadata().setStatus(404);
    }

    public static ExceptionMetadata methodNotAllowed() {

        return new ExceptionMetadata().setStatus(405);
    }
}
