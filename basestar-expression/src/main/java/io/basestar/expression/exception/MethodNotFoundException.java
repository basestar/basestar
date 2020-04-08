package io.basestar.expression.exception;

/*-
 * #%L
 * basestar-expression
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import java.util.Arrays;
import java.util.stream.Collectors;

public class MethodNotFoundException extends RuntimeException {

    private final Class<?> type;

    private final String method;

    private final Class<?>[] args;

    public MethodNotFoundException(final Class<?> type, final String method, final Class<?>[] args) {

        super("Type " + type.getName() + " has no method " + method
                + " with arguments " + Arrays.stream(args).map(Class::getName)
                .collect(Collectors.joining(", ")));

        this.type = type;
        this.method = method;
        this.args = args;
    }
}
