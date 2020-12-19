package io.basestar.spark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Link;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.spark.AbstractSparkTest;
import io.basestar.spark.expand.ExpandStep;
import io.basestar.spark.expand.Expansion;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestExpandStepTransform extends AbstractSparkTest {

    @Test
    void testExpansion() throws IOException {

        final Namespace namespace = Namespace.load(io.basestar.spark.TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final Set<Name> expand = Name.parseSet("ref,ref.ref,ref.ref.ref,link,ref.link,ref.link.ref");

        final ObjectSchema schema = namespace.requireObjectSchema("NestedExpand");
        final Link link = schema.requireLink("link", true);

        final Set<Expansion> step1 = Expansion.expansion(schema, expand);
        assertEquals(ImmutableSet.of(
                new Expansion.OfRef(schema, schema, Name.of("ref"), Name.parseSet("ref,ref.ref,link,link.ref")),
                new Expansion.OfLink(schema, link, Name.of("link"), ImmutableSet.of())
        ), step1);

        final Set<Expansion> step2 = Expansion.expansion(step1);
        assertEquals(ImmutableSet.of(
                new Expansion.OfRef(schema, schema, Name.parse("ref.ref"), Name.parseSet("ref")),
                new Expansion.OfLink(schema, link, Name.parse("ref.link"), Name.parseSet("ref"))
        ), step2);

        final Set<Expansion> step3 = Expansion.expansion(step2);
        assertEquals(ImmutableSet.of(
                new Expansion.OfRef(schema, schema, Name.parse("ref.ref.ref"), ImmutableSet.of()),
                new Expansion.OfRef(schema, schema, Name.parse("ref.link.ref"), ImmutableSet.of())
        ), step3);

        final Set<Expansion> step4 = Expansion.expansion(step3);
        assertEquals(ImmutableSet.of(), step4);
    }

    @Test
    void testExpand() throws IOException {

        final SparkSession session = session();

        final Namespace namespace = Namespace.load(io.basestar.spark.TestExpandTransform.class.getResourceAsStream("schema.yml"));

        final Set<Name> expand = Name.parseSet("ref.link.ref");

        final ObjectSchema schema = namespace.requireObjectSchema("NestedExpand");

        final StructType structType = SparkSchemaUtils.structType(schema);

        final Dataset<Row> dataset = session.createDataset(ImmutableList.of(
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "1", "ref", ReferableSchema.ref("2"))),
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "2", "ref", ReferableSchema.ref("3"))),
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "3", "ref", ReferableSchema.ref("4"))),
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "4", "ref", ReferableSchema.ref("5"))),
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "5", "ref", ReferableSchema.ref("6"))),
                SparkSchemaUtils.toSpark(schema, structType, ImmutableMap.of("id", "6"))
        ), RowEncoder.apply(structType));

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                schema.getQualifiedName(), dataset
        );

        final QueryResolver resolver = new QueryResolver.Automatic(QueryResolver.source(s -> datasets.get(s.getQualifiedName())));

        final Set<Expansion> expansion = Expansion.expansion(schema, expand);
        final ExpandStep step = ExpandStep.from(schema, expansion);
        assertNotNull(step);

        final Dataset<Row> result = step.apply(resolver, dataset);
        final List<Row> results = result.collectAsList();

        System.err.println(result.showString(10, 30, false));
    }
}
