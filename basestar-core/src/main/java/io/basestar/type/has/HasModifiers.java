package io.basestar.type.has;

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

import java.lang.reflect.Modifier;
import java.util.function.Predicate;

public interface HasModifiers {

    int modifiers();

    default boolean isAbstract() {

        return Modifier.isAbstract(modifiers());
    }

    default boolean isPublic() {

        return Modifier.isPublic(modifiers());
    }

    default boolean isPrivate() {

        return Modifier.isPrivate(modifiers());
    }

    default boolean isProtected() {

        return Modifier.isProtected(modifiers());
    }

    default boolean isStatic() {

        return Modifier.isStatic(modifiers());
    }

    default boolean isFinal() {

        return Modifier.isFinal(modifiers());
    }

    static Predicate<HasModifiers> match(final int modifiers) {

        return v -> (v.modifiers() & modifiers) == modifiers;
    }

    static Predicate<HasModifiers> matchNot(final int modifiers) {

        return v -> (v.modifiers() & modifiers) == 0;
    }
}
