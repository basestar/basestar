package io.basestar.graphql.wiring;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;
import io.basestar.schema.Instance;

import java.util.Map;

class InterfaceResolver implements TypeResolver {

    public static final InterfaceResolver INSTANCE = new InterfaceResolver();

    @Override
    public GraphQLObjectType getType(final TypeResolutionEnvironment env) {

        final Map<String, Object> object = env.getObject();
        return env.getSchema().getObjectType(Instance.getSchema(object));
    }
}
