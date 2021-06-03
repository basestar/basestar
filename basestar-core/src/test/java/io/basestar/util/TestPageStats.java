package io.basestar.util;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static io.basestar.util.Page.Stat.APPROX_TOTAL;
import static io.basestar.util.Page.Stat.TOTAL;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPageStats {

    @Test
    void testStatParse() {

        assertEquals(EnumSet.of(TOTAL, APPROX_TOTAL), Page.Stat.parseSet("TOTAL,APPROX_TOTAL"));
        assertEquals(EnumSet.of(TOTAL, APPROX_TOTAL), Page.Stat.parseSet(" TOTAL , , APPROX_TOTAL "));
        assertEquals(EnumSet.of(TOTAL), Page.Stat.parseSet("TOTAL,TOTAL"));
        assertEquals(EnumSet.of(TOTAL, APPROX_TOTAL), Page.Stat.parseSet("total,approx-total"));
        assertEquals(EnumSet.of(TOTAL, APPROX_TOTAL), Page.Stat.parseSet("total,approxTotal"));
        assertEquals(emptySet(), Page.Stat.parseSet(""));
    }

}
