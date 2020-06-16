package io.basestar.mapper.internal;

import io.basestar.expression.Expression;
import io.basestar.expression.type.Coercion;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Id;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.PropertyContext;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IdMapper implements MemberMapper<ObjectSchema.Builder> {

    private final PropertyContext property;

    private final Expression expression;

    private final TypeMapper type;

    public IdMapper(final MappingContext context, final PropertyContext property, final Expression expression) {

        this.property = property;
        this.expression = expression;
        this.type = TypeMapper.from(context, property.type());
    }

    @Override
    public TypeMapper getType() {

        return type;
    }

    @Override
    public void addToSchema(final ObjectSchema.Builder builder) {

        if(expression != null) {
            builder.setId(Id.builder()
                    .setExpression(expression));
        }
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

        if(property.canGet()) {
            final String id = Coercion.toString(type.unmarshall(property.get(source)));
            Instance.setId(target, id);
        }
    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

        if(property.canSet()) {
            final String id = Instance.getId(source);
            property.set(target, type.marshall(id));
        }
    }

    @Override
    public Set<Class<?>> dependencies() {

        return Collections.emptySet();
    }
}
