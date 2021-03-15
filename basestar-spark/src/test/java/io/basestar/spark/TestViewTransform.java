package io.basestar.spark;

/*-
 * #%L
 * basestar-spark
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Namespace;
import io.basestar.spark.database.SparkDatabase;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.transform.ValidateTransform;
import io.basestar.spark.util.CompatibilityUDFs;
import io.basestar.util.Name;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestViewTransform extends AbstractSparkTest {

    @Test
    void testViewTransform() throws IOException {

        final SparkSession session = session();

        final D d1 = new D("a", 1L);
        final D d2 = new D("b", 1L);

        final B b1 = new B("1", d1, 3L, d2);
        final B b2 = new B("2", d1, 2L, d2);
        final B b3 = new B("3", d1, 1L, d2);
        final B b4 = new B("4", d2, 2L, d1);
        final B b5 = new B("5", d2, 4L, d1);
        final B b6 = new B("6", d2, 6L, d1);

        final Dataset<Row> datasetD = session.createDataset(ImmutableList.of(
                d1, d2
        ), Encoders.bean(D.class)).toDF();

        final Dataset<Row> datasetB = session.createDataset(ImmutableList.of(
                b1, b2, b3, b4, b5, b6
        ), Encoders.bean(B.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("B"), datasetB,
                Name.of("D"), datasetD
        );

        final List<AggView> rows = view("AggView", AggView.class, datasets, ImmutableSet.of(Name.of("agg"), Name.of("key"), Name.of("collect")));
        assertEquals(2, rows.size());
        assertTrue(rows.contains(new AggView("a", 8.0, ImmutableList.of(b3.withoutKeys(), b2.withoutKeys(), b1.withoutKeys()))));
        assertTrue(rows.contains(new AggView("b", 14.0, ImmutableList.of(b4.withoutKeys(), b5.withoutKeys(), b6.withoutKeys()))));
    }

    @Test
    void testSqlViewTransform() throws IOException {

        final SparkSession session = session();
        CompatibilityUDFs.register(session);

        final Dataset<Row> datasetA = session.createDataset(ImmutableList.of(
                new A("a:1", new B("b:1")),
                new A("a:2", new B("b:1")),
                new A("a:3", new B("b:2")),
                new A("a:4", new B("b:3")),
                new A("a:5", new B("b:3")),
                new A("a:6", new B("b:3"))
        ), Encoders.bean(A.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("A"), datasetA
        );

        final List<StatsA> rows = view("SqlView", StatsA.class, datasets, ImmutableSet.of());
        assertEquals(3, rows.size());
        assertTrue(rows.contains(new StatsA("b:1", 2L)));
        assertTrue(rows.contains(new StatsA("b:2", 1L)));
        assertTrue(rows.contains(new StatsA("b:3", 3L)));
    }

    @Test
    void testAliasViewTransform() throws IOException {

        final SparkSession session = session();

        final D d1 = new D("a");
        final D d2 = new D("b");
        final D d3 = new D("c");

        final Dataset<Row> datasetB = session.createDataset(ImmutableList.of(
                new B("1", d1, 1L), new B("2", d2, 2L), new B("3", d3, 3L),
                new B("4", d1, 1L), new B("5", d2, 2L), new B("6", d3, 3L)
        ), Encoders.bean(B.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("B"), datasetB
        );

        final List<AliasView> rows = view("AliasView", AliasView.class, datasets);
        assertEquals(3, rows.size());
        rows.forEach(row -> {
            assertNotNull(row.getId());
            assertEquals(2L, row.getCount());
        });
    }

    @Test
    @Disabled
    void testLinkingViewTransform() throws IOException {

        final SparkSession session = session();

        final D d1 = new D("1", 1L);
        final D d2 = new D("2", 2L);
        final D d3 = new D("3", 3L);

        final D g1 = new D("1", 1L);
        final D g2 = new D("2", 2L);
        final D g3 = new D("3", 3L);

        final Dataset<Row> datasetD = session.createDataset(ImmutableList.of(
                d1, d2, d3
        ), Encoders.bean(D.class)).toDF();

        final Dataset<Row> datasetB = session.createDataset(ImmutableList.of(
                new BFlat("1", "1", 1L, "3"), new BFlat("2", "2", 2L, "2"), new BFlat("3", "3", 3L, "1")
        ), Encoders.bean(BFlat.class)).toDF();

        final Dataset<Row> datasetG = session.createDataset(ImmutableList.of(
                g1, g2, g3
        ), Encoders.bean(D.class)).toDF();

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("B"), datasetB,
                Name.of("D"), datasetD,
                Name.of("G"), datasetG
        );

        final List<LinkingView> rows = view("LinkingView", LinkingView.class, datasets, ImmutableSet.of(Name.of("record"), Name.of("key"), Name.of("key2")));
        assertEquals(3, rows.size());
        rows.forEach(row -> {
            assertNotNull(row.getId());
            assertNotNull(row.getRecord());
            assertEquals(row.getId(), row.getRecord().getId());
            assertNotNull(row.getKey());
            assertNotNull(row.getKey2());
        });
    }

    @Test
    @Disabled
    void testLinkingViewToViewTransform() throws IOException {

        final SparkSession session = session();

        final File f1 = new File("f1");

        final FileRow r1 = new FileRow("r1", f1, 100L);

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("File"), session.createDataset(ImmutableList.of(f1), Encoders.bean(File.class)).toDF(),
                Name.of("FileRow"), session.createDataset(ImmutableList.of(r1), Encoders.bean(FileRow.class)).toDF()
        );

        final List<LinkingViewToView> rows = view("LinkingViewToView", LinkingViewToView.class, datasets, ImmutableSet.of(Name.of("headerRows", "rows")));
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).getHeaderRows().getRows().get(0).getRowIndex());
    }

    @Test
    void testLinkingViewToViewInnerTransform() throws IOException {

        final SparkSession session = session();

        final File f1 = new File("f1");

        final FileRow r1 = new FileRow("r1", f1, 100L);

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("File"), session.createDataset(ImmutableList.of(f1), Encoders.bean(File.class)).toDF(),
                Name.of("FileRow"), session.createDataset(ImmutableList.of(r1), Encoders.bean(FileRow.class)).toDF()
        );

        final List<HeaderRows> rows = view("HeaderRows", HeaderRows.class, datasets, ImmutableSet.of(Name.of("rows")));
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).getRows().get(0).getRowIndex());
    }

    @Test
    void testExpressionTransform() throws IOException {

        final SparkSession session = session();

        final File f1 = new File("loremipsum", ImmutableMap.of("a", "b"));

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("File"), session.createDataset(ImmutableList.of(f1), Encoders.bean(File.class)).toDF()
        );

        final List<Expressions> rows = view("Expressions", Expressions.class, datasets);
        assertEquals(1, rows.size());
        final Expressions row = rows.get(0);
        assertEquals("remi", row.getSubstr1());
        assertEquals("ipsum", row.getSubstr2());
        assertEquals("b", row.getMapValue());
        assertEquals("x", row.getLookupValue());
    }

    @Test
    void testConstraints() throws IOException {

        final SparkSession session = session();

        final File f1 = new File("loremipsum", ImmutableMap.of("a", "b"));

        final Map<Name, Dataset<Row>> datasets = ImmutableMap.of(
                Name.of("File"), session.createDataset(ImmutableList.of(f1), Encoders.bean(File.class)).toDF()
        );

        final Namespace namespace = Namespace.load(TestViewTransform.class.getResourceAsStream("schema.yml"));

        final QueryResolver resolver = new QueryResolver.Automatic(QueryResolver.ofSources(schema -> datasets.get(schema.getQualifiedName())));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .build();

        final Dataset<Row> dataset = database.from("Expressions").query();
        final ValidateTransform validate = ValidateTransform.builder()
                .schema(namespace.requireLinkableSchema("Expressions"))
                .build();

        System.err.println(validate.accept(dataset).collectAsList());
    }

    private <T> List<T> view(final String view, final Class<T> as,  final Map<Name, Dataset<Row>> datasets) throws IOException {

        return view(view, as, datasets, ImmutableSet.of());
    }

    private <T> List<T> view(final String view, final Class<T> as,  final Map<Name, Dataset<Row>> datasets, final Set<Name> expand) throws IOException {

        final Namespace namespace = Namespace.load(TestViewTransform.class.getResourceAsStream("schema.yml"));

        final QueryResolver resolver = new QueryResolver.Automatic(QueryResolver.ofSources(schema -> datasets.get(schema.getQualifiedName())));

        final SparkDatabase database = SparkDatabase.builder()
                .resolver(resolver).namespace(namespace)
                .build();

        final Dataset<T> dataset = database.from(view).expand(expand).as(as).query();
        return dataset.collectAsList();
    }
}
