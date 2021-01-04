package io.basestar.schema.util;

import io.basestar.schema.use.Use;

import java.util.Objects;

public interface Widening {

    boolean canWiden(Use<?> from, Use<?> to);

    static Widening none() {

        return None.INSTANCE;
    }

    class None implements Widening {

        public static final None INSTANCE = new None();

        @Override
        public boolean canWiden(final Use<?> from, final Use<?> to) {

            return Objects.equals(from, to);
        }
    }
//
//    class Default implements Widening {
//
//        @Override
//        public boolean canWiden(final Use<?> from, final Use<?> to) {
//
//            return from.visit(new Use.Visitor<Boolean>() {
//                @Override
//                public Boolean visitBoolean(final UseBoolean type) {
//
//                    return to.visit(new Use.Visitor.Defaulting<Boolean>() {
//
//                        @Override
//                        public <T> Boolean visitDefault(final Use<T> type) {
//
//                            return false;
//                        }
//
//                        @Override
//                        public Boolean visitBoolean(final UseBoolean type) {
//
//                            return true;
//                        }
//                    });
//                }
//
//                @Override
//                public Boolean visitInteger(final UseInteger type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitNumber(final UseNumber type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitString(final UseString type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitEnum(final UseEnum type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitRef(final UseRef type) {
//
//                    return null;
//                }
//
//                @Override
//                public <T> Boolean visitArray(final UseArray<T> type) {
//
//                    return null;
//                }
//
//                @Override
//                public <T> Boolean visitSet(final UseSet<T> type) {
//
//                    return null;
//                }
//
//                @Override
//                public <T> Boolean visitMap(final UseMap<T> type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitStruct(final UseStruct type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitBinary(final UseBinary type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitDate(final UseDate type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitDateTime(final UseDateTime type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitView(final UseView type) {
//
//                    return null;
//                }
//
//                @Override
//                public <T> Boolean visitOptional(final UseOptional<T> type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitAny(final UseAny type) {
//
//                    return null;
//                }
//
//                @Override
//                public Boolean visitSecret(final UseSecret type) {
//
//                    return null;
//                }
//
//                @Override
//                public <T> Boolean visitPage(final UsePage<T> type) {
//
//                    return null;
//                }
//            });
//        }
//    }
}
