package io.basestar.jackson.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.jackson.BasestarModule;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestSerde {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    @Data
    public static class TestObject {

        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<String> list;

        @JsonDeserialize(using = AbbrevSetDeserializer.class)
        private Set<String> set;

        private Status status;

        @JsonSerialize(keyUsing = EnumKeySerializer.class)
        @JsonDeserialize(keyUsing = EnumKeyDeserializer.class)
        private Map<Status, Boolean> statusMap;

        private Name name;

        private Map<Name, Boolean> nameMap;

        private LocalDate localDate;

        private Instant instant;

        private Map<Instant, Boolean> instantMap;

        private List<Sort> sort;

        private Expression expression;
    }

    public enum Status {

        NOT_ACTIVE,
        ACTIVE
    }

    @Test
    void testAbbreviated() {

        assertEquals(ImmutableList.of("a"), objectMapper.convertValue(ImmutableMap.of(
                "list", "a"
        ), TestObject.class).getList());

        assertEquals(ImmutableList.of("a", "b"), objectMapper.convertValue(ImmutableMap.of(
                "list", ImmutableList.of("a", "b")
        ), TestObject.class).getList());

        assertEquals(ImmutableSet.of("a"), objectMapper.convertValue(ImmutableMap.of(
                "set", "a"
        ), TestObject.class).getSet());

        assertEquals(ImmutableSet.of("a", "b"), objectMapper.convertValue(ImmutableMap.of(
                "set", ImmutableList.of("a", "b")
        ), TestObject.class).getSet());
    }

    @Test
    void testEnum() {

        assertEquals(Status.NOT_ACTIVE, objectMapper.convertValue(ImmutableMap.of(
                "status", "not-active"
        ), TestObject.class).getStatus());

        assertEquals(Status.NOT_ACTIVE, objectMapper.convertValue(ImmutableMap.of(
                "status", "NOT_ACTIVE"
        ), TestObject.class).getStatus());

        final TestObject t1 = new TestObject();
        t1.setStatus(Status.NOT_ACTIVE);

        assertEquals("not-active", objectMapper.convertValue(t1, Map.class).get("status"));
    }

    @Test
    void testEnumKey() {

        final Map<Status, Boolean> strong = ImmutableMap.of(
                Status.ACTIVE, true,
                Status.NOT_ACTIVE, false
        );
        final Map<String, Boolean> weak = ImmutableMap.of(
                "active", true,
                "not-active", false
        );

        assertEquals(strong, objectMapper.convertValue(ImmutableMap.of(
                "statusMap", weak
        ), TestObject.class).getStatusMap());

        final TestObject t1 = new TestObject();
        t1.setStatusMap(strong);

        assertEquals(weak, objectMapper.convertValue(t1, Map.class).get("statusMap"));
    }

    @Test
    void testName() {

        assertEquals(Name.of("x", "y"), objectMapper.convertValue(ImmutableMap.of(
                "name", "x.y"
        ), TestObject.class).getName());

        final TestObject t1 = new TestObject();
        t1.setName(Name.of("a", "b"));

        assertEquals("a.b", objectMapper.convertValue(t1, Map.class).get("name"));
    }

    @Test
    void testNameKey() {

        final Map<Name, Boolean> strong = ImmutableMap.of(
                Name.parse("a.b"), true,
                Name.parse("x.y"), false
        );
        final Map<String, Boolean> weak = ImmutableMap.of(
                "a.b", true,
                "x.y", false
        );

        assertEquals(strong, objectMapper.convertValue(ImmutableMap.of(
                "nameMap", weak
        ), TestObject.class).getNameMap());

        final TestObject t1 = new TestObject();
        t1.setNameMap(strong);

        assertEquals(weak, objectMapper.convertValue(t1, Map.class).get("nameMap"));
    }

    @Test
    void testLocalDate() {

        final LocalDate now = LocalDate.now();

        assertEquals(now, objectMapper.convertValue(ImmutableMap.of(
                "localDate", ISO8601.toString(now)
        ), TestObject.class).getLocalDate());

        final TestObject t1 = new TestObject();
        t1.setLocalDate(now);

        assertEquals(ISO8601.toString(now), objectMapper.convertValue(t1, Map.class).get("localDate"));
    }

    @Test
    void testInstant() {

        final Instant now = ISO8601.now();

        assertEquals(now, objectMapper.convertValue(ImmutableMap.of(
                "instant", ISO8601.toString(now)
        ), TestObject.class).getInstant());

        final TestObject t1 = new TestObject();
        t1.setInstant(now);

        assertEquals(ISO8601.toString(now), objectMapper.convertValue(t1, Map.class).get("instant"));
    }

    @Test
    void testInstantMap() {

        final Map<Instant, Boolean> strong = ImmutableMap.of(
                Instant.parse("2020-01-01T00:00:00Z"), true,
                Instant.parse("2021-01-01T00:00:00Z"), false
        );
        final Map<String, Boolean> weak = ImmutableMap.of(
                "2020-01-01T00:00:00.000Z", true,
                "2021-01-01T00:00:00.000Z", false
        );

        assertEquals(strong, objectMapper.convertValue(ImmutableMap.of(
                "instantMap", weak
        ), TestObject.class).getInstantMap());

        final TestObject t1 = new TestObject();
        t1.setInstantMap(strong);

        assertEquals(weak, objectMapper.convertValue(t1, Map.class).get("instantMap"));
    }

    @Test
    void testSort() {

        assertEquals(ImmutableList.of(Sort.asc("f1"), Sort.desc("f2")), objectMapper.convertValue(ImmutableMap.of(
                "sort", ImmutableList.of(
                        "f1",
                        "f2 desc"
                )
        ), TestObject.class).getSort());
    }

    @Test
    void testExpression() {

        assertEquals(new Eq(new NameConstant("x"), new Constant(1L)), objectMapper.convertValue(ImmutableMap.of(
                "expression", "x == 1"
        ), TestObject.class).getExpression());
    }
}