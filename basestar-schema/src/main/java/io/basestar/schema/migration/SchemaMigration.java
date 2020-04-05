package io.basestar.schema.migration;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Property;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SchemaMigration implements Serializable {

    private String source;

    private Map<String, Expression> properties;

    public Map<String, Object> migrate(final ObjectSchema sourceSchema, final ObjectSchema targetSchema, final Map<String, Object> source) {

        final Context context = Context.init(source);
        final Map<String, Object> result = new HashMap<>();
        for(final Map.Entry<String, Property> targetEntry : targetSchema.getAllProperties().entrySet()) {
            final String name = targetEntry.getKey();
            //final Property targetProperty = targetEntry.getValue();
            final Property sourceProperty = sourceSchema.getProperty(name, true);
            final Expression migration = properties.get(name);
            if(migration != null) {
                final Object value = migration.evaluate(context);
                result.put(name, value);
            } else if(sourceProperty != null) {
                final Object value = source.get(name);
                result.put(name, value);
            }
        }
        return targetSchema.create(result);
    }
}
