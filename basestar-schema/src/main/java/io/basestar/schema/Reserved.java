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

public class Reserved {

    public static final String DELIMITER = "/";

    public static final String PREFIX = "__";

    public static final String META_PREFIX = "@";

    public static final String ID = "id";

    public static final String SCHEMA = "schema";

    public static final String CREATED = "created";

    public static final String UPDATED = "updated";

    public static final String VERSION = "version";

    public static final String DELETED = Reserved.PREFIX + "deleted";

    public static final String HASH = "hash";

    public static final String THIS = "this";

    public static boolean isReserved(final String name) {

        if(name.startsWith(PREFIX) || (name.indexOf('\0') != -1)) {
            return true;
        } else {
            switch (name) {
//                case ID:
//                case SCHEMA:
//                case CREATED:
//                case UPDATED:
//                case VERSION:
//                case HASH:
                case THIS:
                    return true;
                default:
                    return false;
            }
        }
    }
}
