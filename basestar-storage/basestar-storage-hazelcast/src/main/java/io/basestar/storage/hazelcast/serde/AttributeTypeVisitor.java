package io.basestar.storage.hazelcast.serde;

/*-
 * #%L
 * basestar-storage-hazelcast
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

import io.basestar.schema.use.*;

public class AttributeTypeVisitor implements Use.Visitor<AttributeType<?>> {

    public static final AttributeTypeVisitor INSTANCE = new AttributeTypeVisitor();

    @Override
    public AttributeType<?> visitBoolean(final UseBoolean type) {

        return AttributeType.BOOLEAN;
    }

    @Override
    public AttributeType<?> visitInteger(final UseInteger type) {

        return AttributeType.INTEGER;
    }

    @Override
    public AttributeType<?> visitNumber(final UseNumber type) {

        return AttributeType.NUMBER;
    }

    @Override
    public AttributeType<?> visitString(final UseString type) {

        return AttributeType.STRING;
    }

    @Override
    public AttributeType<?> visitEnum(final UseEnum type) {

        return AttributeType.STRING;
    }

    @Override
    public AttributeType<?> visitRef(final UseRef type) {

        return AttributeType.REF;
    }

    @Override
    public <T> AttributeType<?> visitArray(final UseArray<T> type) {

        return type.getType().visit(ForArray.INSTANCE);
    }

    @Override
    public <T> AttributeType<?> visitSet(final UseSet<T> type) {

        return type.getType().visit(ForArray.INSTANCE);
    }

    @Override
    public <T> AttributeType<?> visitMap(final UseMap<T> type) {

        return AttributeType.encoded(type);
    }

    @Override
    public AttributeType<?> visitStruct(final UseStruct type) {

        return AttributeType.struct(type.getSchema());
    }

    @Override
    public AttributeType<?> visitBinary(final UseBinary type) {

        return AttributeType.BINARY;
    }

    public static class ForArray implements Use.Visitor<AttributeType<?>> {

        public static final ForArray INSTANCE = new ForArray();

        @Override
        public AttributeType<?> visitBoolean(final UseBoolean type) {

            return AttributeType.BOOLEAN_ARRAY;
        }

        @Override
        public AttributeType<?> visitInteger(final UseInteger type) {

            return AttributeType.INTEGER_ARRAY;
        }

        @Override
        public AttributeType<?> visitNumber(final UseNumber type) {

            return AttributeType.NUMBER_ARRAY;
        }

        @Override
        public AttributeType<?> visitString(final UseString type) {

            return AttributeType.STRING_ARRAY;
        }

        @Override
        public AttributeType<?> visitEnum(final UseEnum type) {

            return AttributeType.STRING_ARRAY;
        }

        @Override
        public AttributeType<?> visitRef(final UseRef type) {

            return AttributeType.REF_ARRAY;
        }

        @Override
        public <T> AttributeType<?> visitArray(final UseArray<T> type) {

            return AttributeType.encodedArray(type);
        }

        @Override
        public <T> AttributeType<?> visitSet(final UseSet<T> type) {

            return AttributeType.encodedArray(type);
        }

        @Override
        public <T> AttributeType<?> visitMap(final UseMap<T> type) {

            return AttributeType.encodedArray(type);
        }

        @Override
        public AttributeType<?> visitStruct(final UseStruct type) {

            return AttributeType.structArray(type.getSchema());
        }

        @Override
        public AttributeType<?> visitBinary(final UseBinary type) {

            return AttributeType.encodedArray(type);
        }
    }
}
