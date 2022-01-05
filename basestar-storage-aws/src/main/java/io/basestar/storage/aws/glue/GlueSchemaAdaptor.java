package io.basestar.storage.aws.glue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.*;
import java.util.stream.Collectors;

public class GlueSchemaAdaptor {

    public static final String BUCKET_PREFIX = "__bucket";

    private final ReferableSchema schema;

    private final Map<String, Use<?>> extraMetadata;

    public GlueSchemaAdaptor(final ReferableSchema schema) {

        this(schema, ImmutableMap.of());
    }

    public GlueSchemaAdaptor(final ReferableSchema schema, final Map<String, Use<?>> extraMetadata) {

        this.schema = schema;
        this.extraMetadata = extraMetadata;
    }

    protected String inputFormat() {

        return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    }

    protected String outputFormat() {

        return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    }

    public TableInput tableInput(final String name, final String location) {

        return tableInput(name, location, schema.getExpand());
    }

    public TableInput tableInput(final String name, final String location, final Set<Name> expand) {

        return TableInput.builder()
                .name(name)
                .storageDescriptor(storageDescriptor(location, expand))
                .partitionKeys(partitionKeys())
                .build();
    }

    protected StorageDescriptor storageDescriptor(final String location, final Set<Name> expand) {

        return StorageDescriptor.builder()
                .columns(columns(expand))
                .inputFormat(inputFormat())
                .outputFormat(outputFormat())
                .serdeInfo(serDeInfo())
                .location(location)
                .compressed(true)
                .parameters(parameters())
                .build();
    }

    protected Map<String, String> parameters() {

        return ImmutableMap.of(
                "EXTERNAL", "true",
                "parquet.compress", "SNAPPY"
        );
    }

    protected List<Column> partitionKeys() {

        final List<Column> columns = new ArrayList<>();
        for (int i = 0; i != schema.getEffectiveBucketing().size(); ++i) {
            columns.add(Column.builder()
                    .name(BUCKET_PREFIX + i)
                    .type(type(UseInteger.DEFAULT))
                    .build());
        }
        return columns;
    }

    protected List<Column> columns(final Set<Name> expand) {

        final List<Column> columns = new ArrayList<>(partitionKeys());

        final Map<String, Use<?>> entries = new HashMap<>();
        entries.putAll(schema.layoutSchema(expand));
        entries.putAll(extraMetadata);

        final Map<String, Set<Name>> branches = Name.branch(expand);
        for(final Map.Entry<String, Use<?>> entry : entries.entrySet()) {
            final String name = entry.getKey();
            final Use<?> type = entry.getValue();
            columns.add(Column.builder()
                    .name(name(name))
                    .type(type(type, branches.get(name)))
                    .build());
        }
        return columns;
    }

    protected String refType(final boolean versioned) {

        if(versioned) {
            return structType(ObjectSchema.VERSIONED_REF_SCHEMA, ImmutableSet.of());
        } else {
            return structType(ObjectSchema.REF_SCHEMA, ImmutableSet.of());
        }
    }

    protected String structType(final InstanceSchema schema, final Set<Name> expand) {

        return structType(schema.layoutSchema(expand), expand);
    }

    protected String structType(final Map<String, Use<?>> schema, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final String spec = schema.entrySet().stream().map(e -> name(e.getKey()) + ":" + type(e.getValue(), branches.get(e.getKey())))
                .collect(Collectors.joining(","));
        return "struct<" + spec + ">";
    }

    protected String type(final Use<?> type) {

        return type(type, ImmutableSet.of());
    }

    protected String type(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor<String>() {
            @Override
            public String visitBoolean(final UseBoolean type) {

                return "boolean";
            }

            @Override
            public String visitInteger(final UseInteger type) {

                return "bigint";
            }

            @Override
            public String visitNumber(final UseNumber type) {

                return "double";
            }

            @Override
            public String visitString(final UseString type) {

                return "string";
            }

            @Override
            public String visitEnum(final UseEnum type) {

                return "string";
            }

            @Override
            public String visitRef(final UseRef type) {

                if(expand == null) {
                    return refType(type.isVersioned());
                } else {
                    return structType(type.getSchema(), expand);
                }
            }


            @Override
            public <T> String visitArray(final UseArray<T> type) {

                return "array<" + type(type.getType(), expand) + ">";
            }

            @Override
            public <T> String visitSet(final UseSet<T> type) {

                return "array<" + type(type.getType(), expand) + ">";
            }

            @Override
            public <T> String visitMap(final UseMap<T> type) {

                return "map<" + type(UseString.DEFAULT) + "," + type(type.getType(), expand) + ">";
            }

            @Override
            public String visitStruct(final UseStruct type) {

                return structType(type.getSchema(), expand);
            }

            @Override
            public String visitBinary(final UseBinary type) {

                return "binary";
            }

            @Override
            public String visitDate(final UseDate type) {

                return "date";
            }

            @Override
            public String visitDateTime(final UseDateTime type) {

                return "timestamp";
            }

            @Override
            public String visitView(final UseView type) {

                return structType(type.getSchema(), expand);
            }

            @Override
            public String visitQuery(final UseQuery type) {

                return structType(type.getSchema(), expand);
            }

            @Override
            public <T> String visitOptional(final UseOptional<T> type) {

                return visit(type.getType());
            }

            @Override
            public String visitAny(final UseAny type) {

                return "binary";
            }

            @Override
            public String visitSecret(final UseSecret type) {

                return "binary";
            }

            @Override
            public <T> String visitPage(final UsePage<T> type) {

                return "array<" + type(type.getType(), expand) + ">";
            }

            @Override
            public String visitDecimal(final UseDecimal type) {

                return "decimal(" + type.getPrecision() + "," + type.getScale() + ")";
            }

            @Override
            public String visitComposite(final UseComposite type) {

                return structType(type.getTypes(), expand);
            }
        });
    }

    protected String name(final String name) {

        return name.toLowerCase();
    }

    protected SerDeInfo serDeInfo() {

        return SerDeInfo.builder()
                .serializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .parameters(ImmutableMap.of("serialization.format", "1"))
                .build();
    }
}
