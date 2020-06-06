package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.use.*;

public interface TypeModel {

    String getName();

    // Used for list/set/map value type (this must be on interface else freemarker can't find it)
    default TypeModel getType() {

        throw new UnsupportedOperationException();
    }

    // Used for enum/struct/ref schema (this must be on interface else freemarker can't find it)
    default SchemaModel getSchema() {

        throw new UnsupportedOperationException();
    }

    static TypeModel from(final CodegenSettings settings, final Use<?> use) {

        return use.visit(new Use.Visitor<TypeModel>() {

            @Override
            public TypeModel visitBoolean(final UseBoolean type) {

                return () -> "Boolean";
            }

            @Override
            public TypeModel visitInteger(final UseInteger type) {

                return () -> "Integer";
            }

            @Override
            public TypeModel visitNumber(final UseNumber type) {

                return () -> "Number";
            }

            @Override
            public TypeModel visitString(final UseString type) {

                return () -> "String";
            }

            @Override
            public TypeModel visitEnum(final UseEnum type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return type.getSchema().getName();
                    }

                    @Override
                    public SchemaModel getSchema() {

                        return new EnumSchemaModel(settings, type.getSchema());
                    };
                };
            }

            @Override
            public TypeModel visitRef(final UseRef type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return type.getSchema().getName();
                    }

                    @Override
                    public SchemaModel getSchema() {

                        return new ObjectSchemaModel(settings, type.getSchema());
                    };
                };
            }

            @Override
            public <T> TypeModel visitArray(final UseArray<T> type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return "Array";
                    }

                    @Override
                    public TypeModel getType() {

                        return TypeModel.from(settings, type.getType());
                    };
                };
            }

            @Override
            public <T> TypeModel visitSet(final UseSet<T> type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return "Set";
                    }

                    @Override
                    public TypeModel getType() {

                        return TypeModel.from(settings, type.getType());
                    };
                };
            }

            @Override
            public <T> TypeModel visitMap(final UseMap<T> type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return "Map";
                    }

                    @Override
                    public TypeModel getType() {

                        return TypeModel.from(settings, type.getType());
                    };
                };
            }

            @Override
            public TypeModel visitStruct(final UseStruct type) {

                return new TypeModel() {

                    @Override
                    public String getName() {

                        return type.getSchema().getName();
                    }

                    @Override
                    public SchemaModel getSchema() {

                        return new StructSchemaModel(settings, type.getSchema());
                    };
                };
            }

            @Override
            public TypeModel visitBinary(final UseBinary type) {

                return () -> "Binary";
            }

            @Override
            public TypeModel visitDate(final UseDate type) {

                return () -> "Date";
            }

            @Override
            public TypeModel visitDateTime(final UseDateTime type) {

                return () -> "DateTime";
            }
        });
    }
}
