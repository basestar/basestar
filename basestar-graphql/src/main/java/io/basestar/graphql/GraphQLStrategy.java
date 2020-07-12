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

import io.basestar.database.options.UpdateOptions;
import io.basestar.schema.Link;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.Schema;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.Text;

// FIXME: rename to GraphQL strategy, also create a corresponding RestStrategy

public interface GraphQLStrategy {

    Default DEFAULT = new Default();

    UpdateOptions.Mode updateMode();

    UpdateOptions.Mode patchMode();

    String typeName(Name name);

    default String typeName(Schema<?> type) {

        return typeName(type.getQualifiedName());
    }

    String inputRefTypeName();

    String inputTypeName(Schema<?> type);

    String createInputTypeName(ObjectSchema type);

    String updateInputTypeName(ObjectSchema type);

    String patchInputTypeName(ObjectSchema type);

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

    String patchMethodName(ObjectSchema type);

    String deleteMethodName(ObjectSchema type);

    String queryArgumentName();

    String sortArgumentName();

    String countArgumentName();

    String pagingArgumentName();

    String dataArgumentName();

    String expressionsArgumentName();

    String transactionMethodName();

    String transactionTypeName();

    class Default implements GraphQLStrategy {

        protected String delimiter() {

            // Safe strategy, but won't make the prettiest GQL
            return Reserved.PREFIX;
        }

        @Override
        public UpdateOptions.Mode updateMode() {

            // Gives correct behaviour with immutable properties
            return UpdateOptions.Mode.MERGE;
        }

        @Override
        public UpdateOptions.Mode patchMode() {

            return UpdateOptions.Mode.MERGE_DEEP;
        }

        @Override
        public String typeName(final Name name) {

            return name.transform(Text::upperCamel).toString(delimiter());
        }

        protected String typeName(final Use<?> type) {

            return type.visit(TYPE_NAME_VISITOR);
        }

        protected String inputPrefix() {

            return "Input";
        }

        protected String createInputPrefix() {

            return inputPrefix() + "Create";
        }

        protected String updateInputPrefix() {

            return inputPrefix() + "Update";
        }

        protected String updatePatchPrefix() {

            return inputPrefix() + "Patch";
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
        public String createInputTypeName(final ObjectSchema type) {

            return createInputPrefix() + typeName(type);
        }

        @Override
        public String updateInputTypeName(final ObjectSchema type) {

            return updateInputPrefix() + typeName(type);
        }

        @Override
        public String patchInputTypeName(final ObjectSchema type) {

            return updatePatchPrefix() + typeName(type);
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
        public String patchMethodName(final ObjectSchema type) {

            return "patch" + typeName(type);
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

        @Override
        public String transactionMethodName() {

            return "transaction";
        }

        @Override
        public String transactionTypeName() {

            return "Transaction";
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
            public String visitRef(final UseObject type) {

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

            @Override
            public String visitDate(final UseDate type) {

                return GraphQLUtils.STRING_TYPE;
            }

            @Override
            public String visitDateTime(final UseDateTime type) {

                return GraphQLUtils.STRING_TYPE;
            }
        };
    }
}
