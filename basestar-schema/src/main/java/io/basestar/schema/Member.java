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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.basestar.expression.Context;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.use.Use;
import io.basestar.util.Path;

import java.util.Map;
import java.util.Set;

public interface Member extends Named, Described {

    String VAR_VALUE = "value";

    String getName();

    Visibility getVisibility();

//    void resolve(String name, Schema schema, Namespace namespace);

    Object expand(Object value, Expander expander, Set<Path> expand);

//    Optional<Object> collapse(Object value, Set<Path> expand);

    Set<Path> requireExpand(Set<Path> paths);

    Use<?> typeOf(Path path);

    Set<Path> transientExpand(Path path, Set<Path> expand);

    default boolean isVisible(final Context context, final Object value) {

        final Visibility visibility = getVisibility();
        if(visibility != null) {
            return visibility.apply(context.with(VAR_VALUE, value));
        } else {
            return true;
        }
    }

    Object applyVisibility(Context context, Object value);

    Object evaluateTransients(Context context, Object value, Set<Path> expand);

//    Object evaluateTransients(Context context, Object value, Set<Path> expand);

//    interface Config extends Described {
//
//    }

    public interface Resolver {

        @JsonIgnore
        Map<String, ? extends Member> getDeclaredMembers();

        @JsonIgnore
        Map<String, ? extends Member> getAllMembers();

        Member getMember(String name, boolean inherited);

        default Member requireMember(final String name, final boolean inherited) {

            final Member result = getMember(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }
}
