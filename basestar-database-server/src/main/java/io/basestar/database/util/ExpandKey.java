package io.basestar.database.util;

/*-
 * #%L
 * basestar-database-server
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

import io.basestar.util.Path;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ExpandKey<T> {

    private final T key;

    private final Set<Path> expand;

    public static <T> ExpandKey<T> from(final T key, final Set<Path> expand) {

        assert expand != null;
        return new ExpandKey<>(key, expand);
    }
}
