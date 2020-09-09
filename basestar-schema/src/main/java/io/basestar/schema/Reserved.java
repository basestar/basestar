package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import io.basestar.util.Name;

public class Reserved {

    public static final String DELIMITER = "/";

    public static final String PREFIX = "__";

    public static final String META_PREFIX = "@";

    public static final String META = "@meta";

    public static final String DELETED = Reserved.PREFIX + "deleted";

    public static final String THIS = "this";

    public static final String VALUE = "value";

    public static final Name THIS_NAME = Name.of(THIS);

    public static final Name VALUE_NAME = Name.of(VALUE);

    public static boolean isReserved(final String name) {

        if(name.contains(PREFIX) || name.startsWith(META_PREFIX) || (name.indexOf('\0') != -1)) {
            return true;
        } else {
            switch (name) {
                case THIS:
                    return true;
                default:
                    return false;
            }
        }
    }

    public static boolean isMeta(final String name) {

        return name.startsWith(META_PREFIX);
    }
}
