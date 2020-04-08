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

public enum Consistency implements Comparable<Consistency> {

    ATOMIC,
    QUORUM,
    EVENTUAL,
    ASYNC,
    NONE; /* internal use only */

    public boolean isAtomic() {

        return this == ATOMIC;
    }

    public boolean isQuorum() {

        return this == QUORUM;
    }

    public boolean isEventual() {

        return this == EVENTUAL;
    }

    public boolean isAsync() {

        return this == ASYNC;
    }

    public boolean isStronger(final Consistency other) {

        return ordinal() < other.ordinal();
    }

    public boolean isStrongerOrEqual(final Consistency other) {

        return ordinal() <= other.ordinal();
    }

    public boolean isWeaker(final Consistency other) {

        return ordinal() > other.ordinal();
    }

    public boolean isWeakerOrEqual(final Consistency other) {

        return ordinal() >= other.ordinal();
    }
}
