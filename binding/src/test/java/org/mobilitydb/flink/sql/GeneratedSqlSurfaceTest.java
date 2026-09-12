/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.flink.sql;

import functions.GeneratedFunctions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.flink.sql.types.TFloat;
import org.mobilitydb.flink.sql.types.TGeomPoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the generated Flink SQL surface end to end against libmeos: each query plans against
 * the generated functions, executes on a Flink mini-cluster and reads back its answer. The
 * temporal value is a two-instant float sequence spanning two days in UTC, carried as the
 * hex WKB its generated type writes, against a libmeos on the load path.
 */
class GeneratedSqlSurfaceTest {

    private static TableEnvironment tEnv;
    private static String hexLiteral;
    private static String tfloat;

    @BeforeAll
    static void init() {
        // No-op error handler so a parse error returns rather than terminating the JVM.
        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();
        String hex = TFloat.encode(GeneratedFunctions.tfloat_in(
                "[1@2020-01-01 00:00:00+00, 3@2020-01-03 00:00:00+00]")).form;
        hexLiteral = "'" + hex + "'";
        tfloat = "tfloatFromHexWKB(" + hexLiteral + ")";
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        MobilityFlinkSql.registerAll(tEnv);
    }

    @AfterAll
    static void finalizeMeos() {
        GeneratedFunctions.meos_finalize();
    }

    /** Every row the query emits; a grouped query's last row is its final answer. */
    private static List<Row> rows(String sql) throws Exception {
        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static Object scalar(String sql) throws Exception {
        return rows(sql).get(0).getField(0);
    }

    @Test
    void accessorsReadTheValue() throws Exception {
        assertEquals(2, scalar("SELECT numInstants(" + tfloat + ")"));
        assertEquals(1.0, scalar("SELECT startValue(" + tfloat + ")"));
    }

    @Test
    void overloadsResolveByArgumentType() throws Exception {
        assertEquals(3.0, scalar("SELECT startValue(tAdd(" + tfloat + ", 2.0))"));
        assertEquals(2, scalar("SELECT numInstants(tAdd(" + tfloat + ", " + tfloat + "))"));
    }

    @Test
    void timeValuesCrossAsFlinkTypes() throws Exception {
        assertEquals(Duration.ofHours(48), scalar("SELECT duration(" + tfloat + ")"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"),
                scalar("SELECT startTimestamp(" + tfloat + ")"));
        assertEquals(Instant.parse("2020-01-02T00:00:00Z"),
                scalar("SELECT startTimestamp(shiftTime(" + tfloat + ", INTERVAL '1' DAY))"));
        assertEquals(1, scalar("SELECT numInstants(atTime(" + tfloat
                + ", TO_TIMESTAMP_LTZ(1577836800000, 3)))"));
    }

    @Test
    void constructorsTakeFlinkScalars() throws Exception {
        String inst = "tfloat(CAST(1.5 AS DOUBLE), TO_TIMESTAMP_LTZ(1577836800000, 3))";
        assertEquals(1, scalar("SELECT numInstants(" + inst + ")"));
        assertEquals(1.5, scalar("SELECT startValue(" + inst + ")"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"), scalar("SELECT startTimestamp(" + inst + ")"));
    }

    @Test
    void textConstructorsAndOutput() throws Exception {
        String box = (String) scalar(
                "SELECT asText(tbox_in('TBOXFLOAT XT([1, 2],[2020-01-01, 2020-01-02])'))");
        assertTrue(box.startsWith("TBOXFLOAT XT([1, 2]"), box);
    }

    @Test
    void aReservedNameIsCalledQuoted() throws Exception {
        assertEquals(true,
                scalar("SELECT `overlaps`(floatspan_in('[1, 3]'), floatspan_in('[2, 4]'))"));
    }

    @Test
    void aBuiltinNameResolvesToTheBuiltinUnlessQualified() throws Exception {
        assertThrows(Exception.class, () -> scalar("SELECT lower(floatspan_in('[1, 3]'))"));
        assertEquals(1.0, scalar("SELECT default_database.`lower`(floatspan_in('[1, 3]'))"));
    }

    private static String tgeompoint(String text) {
        return "tgeompointFromHexEWKB('"
                + TGeomPoint.encode(GeneratedFunctions.tgeompoint_in(text)).form + "')";
    }

    @Test
    void arraysCrossAsFlinkArrays() throws Exception {
        assertEquals("{1, 2, 3}", scalar("SELECT intset_out(`set`(ARRAY[3, 1, 2]))"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"), scalar("SELECT startValue(`set`(ARRAY["
                + "TO_TIMESTAMP_LTZ(1577923200000, 3), TO_TIMESTAMP_LTZ(1577836800000, 3)]))"));
        String origin = tgeompoint("[Point(0 0)@2020-01-01, Point(0 0)@2020-01-02]");
        String far = tgeompoint("[Point(3 4)@2020-01-01, Point(3 4)@2020-01-02]");
        String near = tgeompoint("[Point(0 1)@2020-01-01, Point(0 1)@2020-01-02]");
        assertEquals(5.0, scalar("SELECT minDistance(ARRAY[" + origin + "], ARRAY[" + far + "])"));
        assertEquals(1.0, scalar("SELECT minDistance(ARRAY[" + origin + "], ARRAY[" + far + ", "
                + near + "])"));
        assertThrows(Exception.class,
                () -> scalar("SELECT intset_out(`set`(ARRAY[1, CAST(NULL AS INT)]))"));
    }

    @Test
    void valuesGroupAcrossAShuffle() throws Exception {
        List<Row> r = rows("SELECT startValue(v), COUNT(*) FROM (SELECT tfloatFromHexWKB(h) AS v"
                + " FROM (VALUES (" + hexLiteral + "), (" + hexLiteral + ")) AS s(h)) GROUP BY v");
        Row last = r.get(r.size() - 1);
        assertEquals(1.0, last.getField(0));
        assertEquals(2L, last.getField(1));
    }
}
