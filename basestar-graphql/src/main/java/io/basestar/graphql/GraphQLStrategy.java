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
import io.basestar.graphql.transform.GraphQLRequestTransform;
import io.basestar.graphql.transform.GraphQLResponseTransform;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.Text;

public interface GraphQLStrategy {

    Default DEFAULT = new Default();

    UpdateOptions.Mode updateMode();

    UpdateOptions.Mode patchMode();

    String typeName(Name name);

    default String typeName(final Schema type) {

        return typeName(type.getQualifiedName());
    }

    default String missingInterfaceRefTypeName(final Schema type) {

        return "Unresolved" + typeName(type);
    }

    String inputRefTypeName(boolean versioned);

    String inputTypeName(Schema type);

    String createInputTypeName(ObjectSchema type);

    String updateInputTypeName(ObjectSchema type);

    String patchInputTypeName(ObjectSchema type);

    String inputExpressionsTypeName(Schema type);

    String pageTypeName(Schema type);

    String inputMapEntryTypeName(Use<?> type);

    String mapEntryTypeName(Use<?> type);

    String pageItemsFieldName();

    String pagePagingFieldName();

    String readMethodName(ReferableSchema type);

    String subscribeMethodName(ObjectSchema type);

    String subscribeQueryMethodName(ObjectSchema type);

    String queryMethodName(QueryableSchema type);

    String queryLinkMethodName(LinkableSchema type, Link link);

    String queryHistoryMethodName(ReferableSchema type);

    String createMethodName(ObjectSchema type);

    String updateMethodName(ObjectSchema type);

    String patchMethodName(ObjectSchema type);

    String deleteMethodName(ObjectSchema type);

    String queryArgumentName();

    String sortArgumentName();

    String countArgumentName();

    String pagingArgumentName();

    String dataArgumentName();

    String createArgumentName();

    String expressionsArgumentName();

    String consistencyArgumentName();

    String consistencyTypeName();

    String batchMethodName();

    String batchTypeName();

    String idArgumentName();

    String versionArgumentName();

    String pageTotalFieldName();

    String pageApproxTotalFieldName();

    String anyTypeName();

    String dateTypeName();

    String dateTimeTypeName();

    String secretTypeName();

    String decimalTypeName();

    default GraphQLRequestTransform requestTransform() {

        return new GraphQLRequestTransform.Default(this);
    }

    default GraphQLResponseTransform responseTransform() {

        return new GraphQLResponseTransform.Default(this);
    }

    String binaryTypeName();

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

            return type.visit(typeNameVisitor);
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
        public String inputRefTypeName(final boolean versioned) {

            return inputPrefix() + (versioned ? "VersionedRef" : "Ref");
        }

        @Override
        public String inputTypeName(final Schema type) {

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
        public String inputExpressionsTypeName(final Schema type) {

            return inputPrefix() + "Expr" + typeName(type);
        }

        @Override
        public String pageTypeName(final Schema type) {

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
        public String readMethodName(final ReferableSchema type) {

            return "read" + typeName(type);
        }

        @Override
        public String subscribeMethodName(final ObjectSchema type) {

            return "subscribe" + typeName(type);
        }

        @Override
        public String subscribeQueryMethodName(final ObjectSchema type) {

            return "subscribeQuery" + typeName(type);
        }

        @Override
        public String queryMethodName(final QueryableSchema type) {

            return "query" + typeName(type);
        }

        @Override
        public String queryLinkMethodName(final LinkableSchema type, final Link link) {

            return queryMethodName(type) + Text.upperCamel(link.getName());
        }

        @Override
        public String queryHistoryMethodName(final ReferableSchema type) {

            return "history" + typeName(type);
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
        public String createArgumentName() {

            return "create";
        }

        @Override
        public String expressionsArgumentName() {

            return "expressions";
        }

        @Override
        public String consistencyArgumentName() {

            return "consistency";
        }

        @Override
        public String consistencyTypeName() {

            return "BatchConsistency";
        }

        @Override
        public String batchMethodName() {

            return "batch";
        }

        @Override
        public String batchTypeName() {

            return "Batch";
        }

        @Override
        public String idArgumentName() {

            return ObjectSchema.ID;
        }

        @Override
        public String versionArgumentName() {

            return ObjectSchema.VERSION;
        }

        @Override
        public String pageTotalFieldName() {

            return "total";
        }

        @Override
        public String pageApproxTotalFieldName() {

            return "approxTotal";
        }

        @Override
        public String anyTypeName() {

            return "Any";
        }

        @Override
        public String dateTypeName() {

            return "Date";
        }

        @Override
        public String dateTimeTypeName() {

            return "DateTime";
        }

        @Override
        public String secretTypeName() {

            return "Secret";
        }

        @Override
        public String binaryTypeName() {

            return "Binary";
        }

        @Override
        public String decimalTypeName() {

            return "Decimal";
        }

        protected final Use.Visitor<String> typeNameVisitor = new Use.Visitor.Defaulting<String>() {

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
            public <T> String visitStringLike(final UseStringLike<T> type) {

                return GraphQLUtils.STRING_TYPE;
            }

            @Override
            public String visitAny(final UseAny type) {

                return anyTypeName();
            }

            @Override
            public String visitBinary(final UseBinary type) {

                return binaryTypeName();
            }

            @Override
            public String visitDecimal(final UseDecimal type) {

                return decimalTypeName();
            }

            @Override
            public String visitDate(final UseDate type) {

                return dateTypeName();
            }

            @Override
            public String visitDateTime(final UseDateTime type) {

                return dateTimeTypeName();
            }

            @Override
            public String visitSecret(final UseSecret type) {

                return secretTypeName();
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
            public String visitView(final UseView type) {

                return typeName(type.getSchema());
            }
        };
    }
}
