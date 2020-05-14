package io.basestar.graphql;

import io.basestar.schema.Link;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.use.*;
import io.basestar.util.Text;

public interface GraphQLNamingStrategy {

    Default DEFAULT = new Default();

    String typeName(Schema<?> type);

    String inputRefTypeName();

    String inputTypeName(Schema<?> type);

    String inputExpressionsTypeName(Schema<?> type);

    String pageTypeName(Schema<?> type);

    String inputMapEntryTypeName(Use<?> type);

    String mapEntryTypeName(Use<?> type);

    String pageItemsFieldName();

    String pagePagingFieldName();

    String readMethodName(ObjectSchema type);

    String queryMethodName(ObjectSchema type);

    String queryLinkMethodName(ObjectSchema type, Link link);

    String createMethodName(ObjectSchema type);

    String updateMethodName(ObjectSchema type);

    String deleteMethodName(ObjectSchema type);

    String queryArgumentName();

    String sortArgumentName();

    String countArgumentName();

    String pagingArgumentName();

    String dataArgumentName();

    String expressionsArgumentName();

    class Default implements GraphQLNamingStrategy {

        @Override
        public String typeName(final Schema<?> type) {

            return Text.upperCamel(type.getName());
        }

        protected String typeName(final Use<?> type) {

            return type.visit(TYPE_NAME_VISITOR);
        }

        protected String inputPrefix() {

            return "Input";
        }

        protected String entryPrefix() {

            return "Entry";
        }

        protected String arrayPrefix() {

            return "Array";
        }

        @Override
        public String inputRefTypeName() {

            return inputPrefix() + "Ref";
        }

        @Override
        public String inputTypeName(final Schema<?> type) {

            return inputPrefix() + typeName(type);
        }

        @Override
        public String inputExpressionsTypeName(final Schema<?> type) {

            return inputPrefix() + "Expr" + typeName(type);
        }

        @Override
        public String pageTypeName(final Schema<?> type) {

            return typeName(type) + "Page";
        }

        @Override
        public String inputMapEntryTypeName(final Use<?> type) {

            return inputPrefix() + mapEntryTypeName(type);
        }

        @Override
        public String mapEntryTypeName(final Use<?> type) {

            return entryPrefix() + typeName(type);
        }

        @Override
        public String pageItemsFieldName() {

            return "items";
        }

        @Override
        public String pagePagingFieldName() {

            return "paging";
        }

        @Override
        public String readMethodName(final ObjectSchema type) {

            return "read" + typeName(type);
        }

        @Override
        public String queryMethodName(final ObjectSchema type) {

            return "query" + typeName(type);
        }

        @Override
        public String queryLinkMethodName(final ObjectSchema type, final Link link) {

            return queryMethodName(type) + Text.upperCamel(link.getName());
        }

        @Override
        public String createMethodName(final ObjectSchema type) {

            return "create" + typeName(type);
        }

        @Override
        public String updateMethodName(final ObjectSchema type) {

            return "update" + typeName(type);
        }

        @Override
        public String deleteMethodName(final ObjectSchema type) {

            return "delete" + typeName(type);
        }

        @Override
        public String queryArgumentName() {

            return "query";
        }

        @Override
        public String sortArgumentName() {

            return "sort";
        }

        @Override
        public String countArgumentName() {

            return "count";
        }

        @Override
        public String pagingArgumentName() {

            return pagePagingFieldName();
        }

        @Override
        public String dataArgumentName() {

            return "data";
        }

        @Override
        public String expressionsArgumentName() {

            return "expressions";
        }

        protected final Use.Visitor<String> TYPE_NAME_VISITOR = new Use.Visitor<String>() {

            @Override
            public String visitBoolean(final UseBoolean type) {

                return GraphQLUtils.BOOLEAN_TYPE;
            }

            @Override
            public String visitInteger(final UseInteger type) {

                return GraphQLUtils.INT_TYPE;
            }

            @Override
            public String visitNumber(final UseNumber type) {

                return GraphQLUtils.FLOAT_TYPE;
            }

            @Override
            public String visitString(final UseString type) {

                return GraphQLUtils.STRING_TYPE;
            }

            @Override
            public String visitEnum(final UseEnum type) {

                return typeName(type.getSchema());
            }

            @Override
            public String visitRef(final UseRef type) {

                return typeName(type.getSchema());
            }

            @Override
            public <T> String visitArray(final UseArray<T> type) {

                return arrayPrefix() + type.getType().visit(this);
            }

            @Override
            public <T> String visitSet(final UseSet<T> type) {

                return arrayPrefix() + type.getType().visit(this);
            }

            @Override
            public <T> String visitMap(final UseMap<T> type) {

                return entryPrefix() + type.getType().visit(this);
            }

            @Override
            public String visitStruct(final UseStruct type) {

                return typeName(type.getSchema());
            }

            @Override
            public String visitBinary(final UseBinary type) {

                return GraphQLUtils.STRING_TYPE;
            }
        };
    }
}