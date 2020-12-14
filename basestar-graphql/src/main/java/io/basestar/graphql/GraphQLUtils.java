package io.basestar.graphql;

/*-
 * #%L
 * basestar-graphql
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

import com.google.common.collect.ImmutableSet;
import graphql.GraphQLContext;
import graphql.language.*;
import io.basestar.auth.Caller;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Name;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphQLUtils {

    public static final String ID_TYPE = "ID";

    public static final String STRING_TYPE = "String";

    public static final String INT_TYPE = "Int";

    public static final String FLOAT_TYPE = "Float";

    public static final String BOOLEAN_TYPE = "Boolean";

    public static final String MAP_KEY = "key";

    public static final String MAP_VALUE = "value";

    public static final String QUERY_TYPE = "Query";

    public static final String MUTATION_TYPE = "Mutation";

    public static final String SUBSCRIPTION_TYPE = "Subscription";

    private GraphQLUtils() {

    }

    public static Set<Name> paths(final InstanceSchema schema, final SelectionSet selections) {

        return selections.getSelections().stream()
                .flatMap(v -> paths(schema, v).stream())
                .collect(Collectors.toSet());
    }

    public static Set<Name> paths(final InstanceSchema schema, final Selection<?> selection) {

        if (selection instanceof Field) {
            final Field field = (Field) selection;
            final String name = field.getName();

            if (schema.metadataSchema().containsKey(name)) {
                return Collections.singleton(Name.of(name));
            }

            if (schema instanceof Link.Resolver) {
                final Link link = ((Link.Resolver) schema).getLink(name, true);
                if (link != null) {
                    return paths(link.getSchema(), field.getSelectionSet()).stream()
                            .map(v -> Name.of(name).with(v)).collect(Collectors.toSet());
                }
            }
            final Property property = schema.requireProperty(name, true);
            return paths(property.getType(), Name.of(name), field.getSelectionSet());

        } else {
            throw new IllegalStateException();
        }
    }

    protected static Set<Name> paths(final Use<?> type, final Name parent, final SelectionSet selections) {

        return type.visit(new Use.Visitor.Defaulting<Set<Name>>() {

            @Override
            public <T> Set<Name> visitDefault(final Use<T> type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Name> visitInstance(final UseInstance type) {

                return paths(type.getSchema(), selections).stream()
                        .map(parent::with).collect(Collectors.toSet());
            }

            @Override
            public <V, T extends Collection<V>> Set<Name> visitCollection(final UseCollection<V, T> type) {

                return paths(type.getType(), parent, selections);
            }

            @Override
            public <T> Set<Name> visitMap(final UseMap<T> type) {

                final Set<Name> names = new HashSet<>();
                for (final Selection<?> selection : selections.getSelections()) {
                    final Field field = (Field) selection;
                    final String name = field.getName();
                    if (MAP_KEY.equals(name)) {
                        names.add(parent.with("*"));
                    } else if (MAP_VALUE.equals(name)) {
                        names.addAll(paths(type.getType(), parent.with("*"), field.getSelectionSet()));
                    }
                }
                return names;
            }
        });
    }

    public static Argument arg(final Field field, final String name) {

        return field.getArguments().stream().filter(v -> v.getName().equals(name))
                .findFirst().orElse(null);
    }

    public static Caller caller(final GraphQLContext context) {

        if (context != null) {
            final Caller caller = context.get("caller");
            if (caller != null) {
                return caller;
            }
        }
        return Caller.ANON;
    }

    public static SubscriberContext subscriber(final GraphQLContext context) {

        if (context != null) {
            final SubscriberContext sub = context.get("subscriber");
            if (sub != null) {
                return sub;
            }
        }
        throw new IllegalStateException("Subscription not available");
    }


    public static Object fromValue(final Object value, final Map<String, Object> variables) {

        if (value instanceof Value<?>) {
            return fromValue((Value<?>) value, variables);
        } else {
            return value;
        }
    }

    public static Object fromValue(final Value<?> value, final Map<String, Object> variables) {

        if (value == null || value instanceof NullValue) {
            return null;
        } else if (value instanceof ObjectValue) {
            final Map<String, Object> result = new HashMap<>();
            ((ObjectValue) value).getObjectFields().forEach(f -> result.put(f.getName(), fromValue(f.getValue(), variables)));
            return result;
        } else if (value instanceof ArrayValue) {
            return ((ArrayValue) value).getValues().stream()
                    .map(v -> fromValue(v, variables))
                    .collect(Collectors.toList());
        } else if (value instanceof StringValue) {
            return ((StringValue) value).getValue();
        } else if (value instanceof BooleanValue) {
            return ((BooleanValue) value).isValue();
        } else if (value instanceof IntValue) {
            return ((IntValue) value).getValue().longValue();
        } else if (value instanceof FloatValue) {
            return ((FloatValue) value).getValue().doubleValue();
        } else if (value instanceof EnumValue) {
            return ((EnumValue) value).getName();
        } else if (value instanceof VariableReference) {
            return fromValue(variables.get(((VariableReference) value).getName()), variables);
        } else {
            throw new UnsupportedOperationException("Cannot understand " + value.getClass());
        }
    }

    public static Set<Name> expand(final GraphQLStrategy strategy, final Namespace namespace, final InstanceSchema schema, final SelectionSet selections) {

        if (selections == null || selections.getSelections() == null) {
            return Collections.emptySet();
        }
        return selections.getSelections().stream()
                .flatMap(selection -> {
                    if (selection instanceof Field) {
                        final Field field = (Field) selection;
                        final Name name = Name.of(field.getName());
                        final Member member = schema.getMember(field.getName(), true);
                        if(member != null) {
                            final Stream<Name> result = expand(strategy, namespace, member.getType(), field.getSelectionSet()).stream().map(name::with);
                            if(member instanceof Transient) {
                                return Stream.concat(Stream.of(name), result);
                            } else {
                                return result;
                            }
                        } else {
                            return Stream.empty();
                        }
                    } else if (selection instanceof InlineFragment) {
                        final InlineFragment fragment = (InlineFragment) selection;
                        if(fragment.getTypeCondition() != null) {
                            final String name = fragment.getTypeCondition().getName();
                            final InstanceSchema resolvedSchema = namespace.requireInstanceSchema(name);
                            return expand(strategy, namespace, resolvedSchema, fragment.getSelectionSet()).stream();
                        } else {
                            return expand(strategy, namespace, schema, fragment.getSelectionSet()).stream();
                        }
                    } else {
                        return Stream.empty();
                    }
                })
                .filter(v -> !v.isEmpty())
                .collect(Collectors.toSet());
    }

    public static Set<Name> expand(final GraphQLStrategy strategy, final Namespace namespace, final Use<?> type, final SelectionSet selections) {

        return type.visit(new Use.Visitor.Defaulting<Set<Name>>() {

            @Override
            public <T> Set<Name> visitDefault(final Use<T> type) {

                return Collections.emptySet();
            }

            @Override
            public <T> Set<Name> visitMap(final UseMap<T> type) {

                final Field field = findField(selections, GraphQLUtils.MAP_VALUE);
                if(field != null) {
                    return expand(strategy, namespace, type.getType(), field.getSelectionSet());
                } else {
                    return Collections.emptySet();
                }
            }

            @Override
            public <T> Set<Name> visitPage(final UsePage<T> type) {

                final Field field = findField(selections, strategy.pageItemsFieldName());
                if(field != null) {
                    return expand(strategy, namespace, type.getType(), field.getSelectionSet());
                } else {
                    return Collections.emptySet();
                }
            }

            @Override
            public <V, T extends Collection<V>> Set<Name> visitCollection(final UseCollection<V, T> type) {

                return expand(strategy, namespace, type.getType(), selections);
            }

            @Override
            public Set<Name> visitLinkable(final UseLinkable type) {

                final Set<Name> result = new HashSet<>();
                result.add(Name.empty());
                result.addAll(expand(strategy, namespace, type.getSchema(), selections));
                return result;
            }
        });
    }

    public static Field findField(final SelectionSet selections, final String name) {

        if(selections == null || selections.getSelections() == null) {
            return null;
        }
        for(final Selection<?> selection : selections.getSelections()) {
            if(selection instanceof Field) {
                final Field field = (Field)selection;
                if(field.getName().equals(name)) {
                    return field;
                }
            } else if(selection instanceof InlineFragment) {
                final InlineFragment fragment = (InlineFragment)selection;
                final Field field = findField(fragment.getSelectionSet(), name);
                if(field != null) {
                    return field;
                }
            }
        }
        return null;
    }
}