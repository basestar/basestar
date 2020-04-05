package io.basestar.storage.hazelcast.serde;

import io.basestar.schema.use.*;

public class AttributeTypeVisitor implements Use.Visitor<AttributeType<?>> {

    public static AttributeTypeVisitor INSTANCE = new AttributeTypeVisitor();

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
    public AttributeType<?> visitRef(final UseObject type) {

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

        public static ForArray INSTANCE = new ForArray();

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
        public AttributeType<?> visitRef(final UseObject type) {

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
