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
import com.fasterxml.jackson.annotation.JsonInclude;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseAny;
import io.basestar.schema.use.ValueContext;
import io.basestar.schema.use.Widening;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Member extends Named, Described, Serializable, Extendable {

    String VAR_VALUE = "value";

    boolean supportsTrivialJoin(Set<Name> expand);

    boolean canModify(Member member, Widening widening);

    boolean canCreate();

    interface Descriptor extends Described, Extendable {

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Visibility getVisibility();

        interface Self<M extends Member> extends Descriptor {

            M self();

            @Override
            default Visibility getVisibility() {

                return self().getVisibility();
            }

            @Override
            default String getDescription() {

                return self().getDescription();
            }

            @Override
            default Map<String, Serializable> getExtensions() {

                return self().getExtensions();
            }
        }
    }

    interface Builder<B extends Builder<B>> extends Descriptor, Extendable.Builder<B> {

    }

    interface Resolver {

        @JsonIgnore
        Map<String, ? extends Member> getDeclaredMembers();

        @JsonIgnore
        Map<String, ? extends Member> getMembers();

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

    Use<?> getType();

    Optional<Use<?>> layout(Set<Name> expand);

    Visibility getVisibility();

    void expand(Name parent, Expander expander, Set<Name> expand);

    Object expand(Name parent, Object value, Expander expander, Set<Name> expand);

    Set<Name> requiredExpand(Set<Name> names);

    <T> Use<T> typeOf(Name name);

    Type javaType(Name name);

    Set<Name> transientExpand(Name name, Set<Name> expand);

    Object applyVisibility(Context context, Object value);

    Object evaluateTransients(Context context, Object value, Set<Name> expand);

    Set<Expression> refQueries(Name otherSchemaName, Set<Name> expand, Name name);

    Set<Name> refExpand(Name otherSchemaName, Set<Name> expand);

    default void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        getType().collectDependencies(expand, out);
    }

    default boolean isVisible(final Context context, final Object value) {

        final Visibility visibility = getVisibility();
        if(visibility != null) {
            return visibility.apply(context.with(VAR_VALUE, value));
        } else {
            return true;
        }
    }

    default boolean isAlwaysVisible() {

        final Visibility visibility = getVisibility();
        return visibility == null || visibility.isAlwaysVisible();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    default boolean isAlwaysHidden() {

        final Visibility visibility = getVisibility();
        return visibility != null && visibility.isAlwaysHidden();
    }

    default io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return getType().openApi(expand).description(getDescription());
    }

    Object create(ValueContext context, Object value, Set<Name> expand);

    Descriptor descriptor();

    static Use<?> type(final Use<?> type, final Expression expression, final InferenceContext context) {

        if(type == null) {
            if(context != null && expression != null) {
                final Use<?> inferredType = new InferenceVisitor(context).visit(expression);
                if(inferredType instanceof UseAny) {
                    throw new IllegalStateException("Cannot infer type from expression " + expression);
                } else {
                    return inferredType;
                }
            } else {
                throw new IllegalStateException("Property type or expression must be specified");
            }
        } else {
            return type;
        }
    }
}
