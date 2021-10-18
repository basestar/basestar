package io.basestar.storage.sql.dialect;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.from.FromSql;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.Optional;


public class SnowflakeDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return SQLDialect.POSTGRES;
    }

    @Override
    protected DataType<?> jsonType() {

        return SQLDataType.LONGVARCHAR;
    }

    @Override
    protected Object castJson(final String value) {

        return value;
    }

    @Override
    public boolean supportsConstraints() {

        return false;
    }

    @Override
    public boolean supportsIndexes() {

        return false;
    }

    @Override
    public boolean supportsILike() {

        return true;
    }

    @Override
    protected boolean isJsonEscaped() {

        return false;
    }

    @Override
    public Optional<? extends Field<?>> missingMetadataValue(final LinkableSchema schema, final String name) {

        if (schema instanceof ViewSchema && ViewSchema.ID.equals(name)) {
            return Optional.of(DSL.inline((String) null));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean supportsMaterializedView(final ViewSchema schema) {

        return schema.getFrom() instanceof FromSql && (((FromSql) schema.getFrom()).getUsing().size() == 1);
    }

    @Override
    public String createFunctionDDLLanguage(final String language) {

        if ("sql".equalsIgnoreCase(language)) {
            return "";
        } else {
            return super.createFunctionDDLLanguage(language);
        }
    }
}
