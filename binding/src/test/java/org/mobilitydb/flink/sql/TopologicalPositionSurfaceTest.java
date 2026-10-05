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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Calls every topological and position function of the generated Flink SQL surface by its
 * class-prefixed name, unquoted, on the mini-cluster and libmeos #GeneratedSqlSurfaceTest runs
 * the surface on. The topological functions take the prefix of the class whose values or
 * bounding boxes they compare, as the position functions do, the names the portable dialect
 * chapter of the MobilityDB manual gives: set, span, spanset, tbox, stbox and tpcbox, and a
 * base type for same. Each overload the surface registers is called once over values read
 * from their text, a box argument being the box of the value beside it, and answers a
 * Boolean; each operator answers its known value over two spans, two time spans and two
 * spatiotemporal boxes.
 */
class TopologicalPositionSurfaceTest {

    private static TableEnvironment tEnv;

    /** Each name the surface registers, with its function class. */
    private static final Map<String, Class<?>> REGISTERED = new LinkedHashMap<>();

    /** The topological names, the operators each prefix takes. */
    private static final Map<String, List<String>> TOPOLOGICAL = new LinkedHashMap<>();

    static {
        List<String> all = List.of("Overlaps", "Contains", "Contained", "Same", "Adjacent");
        TOPOLOGICAL.put("stbox", all);
        TOPOLOGICAL.put("tbox", all);
        TOPOLOGICAL.put("span", all);
        TOPOLOGICAL.put("tpcbox", all);
        TOPOLOGICAL.put("set", List.of("Overlaps", "Contains", "Contained"));
        TOPOLOGICAL.put("spanset", List.of("Overlaps", "Contains", "Contained", "Adjacent"));
        for (String base : List.of("cbuffer", "npoint", "pose", "posechain")) {
            TOPOLOGICAL.put(base, List.of("Same"));
        }
    }

    private static final String OPS = "Overlaps|Contains|Contained|Same|Adjacent"
            + "|Left|Overleft|Right|Overright|Below|Overbelow|Above|Overabove"
            + "|Front|Overfront|Back|Overback|Before|Overbefore|After|Overafter";

    private static final Pattern FAMILY = Pattern.compile(
            "(set|span|spanset|tbox|stbox|tpcbox|cbuffer|npoint|pose|posechain)(" + OPS + ")");

    /** The position operators reading the third dimension, called over values having one. */
    private static final Set<String> Z_OPS = Set.of("Front", "Overfront", "Back", "Overback");

    private static final String PCPOINT = "010100000064000000C80000002C010000";
    private static final String PCPATCH =
            "01010000000000000002000000640000006400000064000000C8000000C8000000C8000000";

    /** The text of a value of each SQL type, as the reader of #READER reads it. */
    private static final Map<String, String> TEXT = new LinkedHashMap<>();

    /**
     * The reader of a SQL type whose text is not read by its FromText: the hex Well-Known
     * Binary of a point cloud value and the hex extended one of a geometry and a geography.
     */
    private static final Map<String, String> READER = Map.of(
            "pcpoint", "pcpointFromHexWKB", "pcpatch", "pcpatchFromHexWKB",
            "geometry", "geometryFromHexEWKB", "geography", "geographyFromHexEWKB");

    /** The text of a spatial value with a third dimension, where it differs. */
    private static final Map<String, String> TEXT_Z = new LinkedHashMap<>();

    static {
        TEXT.put("intset", "{1, 2}");
        TEXT.put("bigintset", "{1, 2}");
        TEXT.put("floatset", "{1.5, 2.5}");
        TEXT.put("textset", "{\"a\", \"b\"}");
        TEXT.put("dateset", "{2020-01-01, 2020-01-02}");
        TEXT.put("tstzset", "{2020-01-01, 2020-01-02}");
        TEXT.put("geomset", "{Point(1 1), Point(2 2)}");
        TEXT.put("geogset", "{Point(1 1), Point(2 2)}");
        TEXT.put("cbufferset", "{\"Cbuffer(Point(1 1),0.5)\", \"Cbuffer(Point(2 2),0.5)\"}");
        TEXT.put("npointset", "{\"Npoint(1,0.5)\", \"Npoint(2,0.5)\"}");
        TEXT.put("poseset", "{\"Pose(Point(1 1),0.5)\", \"Pose(Point(2 2),0.5)\"}");
        TEXT.put("posechainset", "{\"PoseChain(Pose(Point(0 0), 0), Pose(Point(10 0), 0))\"}");
        TEXT.put("jsonbset", "{\"{\\\"a\\\": 1}\"}");
        TEXT.put("h3indexset", "{8a2a1072b59ffff}");
        TEXT.put("quadbinset", "{4813ffffffffffff, 4817ffffffffffff}");
        TEXT.put("s2cellset", "{47c3c444c000000c, 47c3c444c0000004}");
        TEXT.put("pcpointset", "{\"" + PCPOINT + "\"}");
        TEXT.put("pcpatchset", "{\"" + PCPATCH + "\"}");
        TEXT.put("intspan", "[1, 3]");
        TEXT.put("bigintspan", "[1, 3]");
        TEXT.put("floatspan", "[1.5, 3.5]");
        TEXT.put("datespan", "[2020-01-01, 2020-01-03]");
        TEXT.put("tstzspan", "[2020-01-01, 2020-01-03]");
        TEXT.put("intspanset", "{[1, 3]}");
        TEXT.put("bigintspanset", "{[1, 3]}");
        TEXT.put("floatspanset", "{[1.5, 3.5]}");
        TEXT.put("datespanset", "{[2020-01-01, 2020-01-03]}");
        TEXT.put("tstzspanset", "{[2020-01-01, 2020-01-03]}");
        TEXT.put("tbox", "TBOXFLOAT XT([1, 3],[2020-01-01, 2020-01-03])");
        TEXT.put("stbox", "STBOX XT(((1,1),(3,3)),[2020-01-01, 2020-01-03])");
        TEXT.put("tpcbox", "TPCBOX(XT(((0,0),(10,10)),[2020-01-01, 2020-01-31]), 1)");
        TEXT.put("cbuffer", "Cbuffer(Point(1 1), 0.5)");
        TEXT.put("npoint", "Npoint(1, 0.5)");
        TEXT.put("nsegment", "NSegment(1, 0.2, 0.6)");
        TEXT.put("pose", "Pose(Point(1 1), 0.5)");
        TEXT.put("posechain", "PoseChain(Pose(Point(0 0), 0), Pose(Point(10 0), 0))");
        TEXT.put("tint", "[1@2020-01-01, 2@2020-01-02]");
        TEXT.put("tbigint", "[1@2020-01-01, 2@2020-01-02]");
        TEXT.put("tfloat", "[1.5@2020-01-01, 2.5@2020-01-02]");
        TEXT.put("tbool", "[t@2020-01-01, f@2020-01-02]");
        TEXT.put("ttext", "[\"a\"@2020-01-01, \"b\"@2020-01-02]");
        TEXT.put("tjsonb", "\"{\\\"a\\\": 1}\"@2020-01-01");
        TEXT.put("tgeompoint", "[Point(1 1)@2020-01-01, Point(2 2)@2020-01-02]");
        TEXT.put("tgeogpoint", "[Point(1 1)@2020-01-01, Point(2 2)@2020-01-02]");
        TEXT.put("tgeometry", "Point(1 1)@2020-01-01");
        TEXT.put("tgeography", "Point(1 1)@2020-01-01");
        TEXT.put("tcbuffer", "Cbuffer(Point(1 1), 0.5)@2020-01-01");
        TEXT.put("tnpoint", "Npoint(1, 0.5)@2020-01-01");
        TEXT.put("tpose", "Pose(Point(1 1), 0.1)@2020-01-01");
        TEXT.put("tposechain", "PoseChain(Pose(Point(1 2), 0.1))@2020-01-01");
        TEXT.put("trgeometry", "Polygon((0 0, 1 0, 1 1, 0 1, 0 0));Pose(Point(5 5),0)@2020-01-01");
        TEXT.put("th3index", "831c02fffffffff@2020-01-01");
        TEXT.put("tquadbin", "480fffffffffffff@2020-01-01");
        TEXT.put("ts2cell", "47c3c3@2020-01-01");
        TEXT.put("tpcpoint", PCPOINT + "@2020-01-01");
        TEXT.put("tpcpatch", PCPATCH + "@2020-01-01");
        TEXT.put("geometry", "0101000000000000000000F03F000000000000F03F");
        TEXT.put("geography", "0101000020E6100000000000000000F03F000000000000F03F");
        TEXT.put("jsonb", "{\"a\": 1}");
        TEXT.put("h3index", "8a2a1072b59ffff");
        TEXT.put("quadbin", "4813ffffffffffff");
        TEXT.put("s2cell", "47c3c444c000000c");
        TEXT.put("pcpoint", PCPOINT);
        TEXT.put("pcpatch", PCPATCH);

        TEXT_Z.put("stbox", "STBOX ZT(((1,1,1),(3,3,3)),[2020-01-01, 2020-01-03])");
        TEXT_Z.put("tpcbox", "TPCBOX(ZT(((0,0,0),(10,10,10)),[2020-01-01, 2020-01-31]), 1)");
        TEXT_Z.put("geomset", "{Point(1 1 1), Point(2 2 2)}");
        TEXT_Z.put("geogset", "{Point(1 1 1), Point(2 2 2)}");
        TEXT_Z.put("tgeompoint", "[Point(1 1 1)@2020-01-01, Point(2 2 2)@2020-01-02]");
        TEXT_Z.put("tgeogpoint", "[Point(1 1 1)@2020-01-01, Point(2 2 2)@2020-01-02]");
        TEXT_Z.put("tgeometry", "Point(1 1 1)@2020-01-01");
        TEXT_Z.put("tgeography", "Point(1 1 1)@2020-01-01");
        TEXT_Z.put("tpose", "Pose(Point Z(1 1 1), 1, 0, 0, 0)@2020-01-01");
        TEXT_Z.put("tposechain", "PoseChain(Pose(Point Z(1 1 1), 1, 0, 0, 0))@2020-01-01");
        TEXT_Z.put("trgeometry",
                "Polygon Z((0 0 0,2 0 0,2 1 0,0 1 0,0 0 0));Pose(Point Z(1 1 0), 1, 0, 0, 0)@2020-01-01");
    }

    /**
     * States the schema of pcid 1 and registers the surface. MEOS is set up by the generated
     * wrappers themselves, which install the handler that turns a MEOS error into the
     * exception each call below expects, so this test installs none of its own.
     */
    @BeforeAll
    static void init() {
        // The point cloud values name pcid 1, whose schema the libmeos installation ships
        // beside its library directory, the one surefire puts on the load path.
        Path lib = Paths.get(System.getenv("LD_LIBRARY_PATH").split(":")[0]);
        GeneratedFunctions.meos_set_pointcloud_schemas_xml(
                lib.resolveSibling("share").resolve("pointcloud_schemas.xml").toString());
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // #registerAll registers through the environment it is given; a proxy forwarding every
        // call records each name with its function class, as the registrar states them
        TableEnvironment recording = (TableEnvironment) Proxy.newProxyInstance(
                TableEnvironment.class.getClassLoader(), new Class<?>[] {TableEnvironment.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("createTemporaryFunction") && args.length == 2
                            && args[0] instanceof String && args[1] instanceof Class) {
                        REGISTERED.put((String) args[0], (Class<?>) args[1]);
                    }
                    try {
                        return method.invoke(tEnv, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        MobilityFlinkSql.registerAll(recording);
    }

    /** The first field of the first row, as #GeneratedSqlSurfaceTest reads an answer. */
    private static Object scalar(String sql) throws Exception {
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            return it.next().getField(0);
        }
    }

    /** The SQL type a generated value class carries, its class name in lower case. */
    private static String sqlType(Class<?> c) {
        return c.getSimpleName().toLowerCase();
    }

    /** True when the surface converts a value of class {@code from} to the box class {@code box}. */
    private static boolean converts(Class<?> box, Class<?> from) {
        String type = sqlType(box);
        try {
            Class<?> fn = Class.forName("org.mobilitydb.flink.sql.functions."
                    + type.substring(0, 1).toUpperCase() + type.substring(1));
            return Arrays.stream(fn.getMethods()).anyMatch(m -> m.getName().equals("eval")
                    && m.getParameterCount() == 1 && m.getParameterTypes()[0] == from);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /** The SQL expression of an argument of class {@code c} beside one of class {@code other}. */
    private static String arg(Class<?> c, Class<?> other, boolean z) {
        if (c == Integer.class) {
            return "CAST(2 AS INT)";
        }
        if (c == Long.class) {
            return "CAST(2 AS BIGINT)";
        }
        if (c == Double.class) {
            return "CAST(2.5 AS DOUBLE)";
        }
        if (c == java.time.Instant.class) {
            return "TO_TIMESTAMP_LTZ(1577923200000, 3)";
        }
        if (c == java.time.LocalDate.class) {
            return "DATE '2020-01-02'";
        }
        if (c == String.class) {
            return "'a'";
        }
        if (c == Boolean.class) {
            return "TRUE";
        }
        String type = sqlType(c);
        if ((type.equals("stbox") || type.equals("tbox")) && other != null && converts(c, other)) {
            return type + "(" + arg(other, null, z) + ")";
        }
        String text = z && TEXT_Z.containsKey(type) ? TEXT_Z.get(type) : TEXT.get(type);
        if (text == null) {
            throw new IllegalStateException("no text stated for the SQL type " + type);
        }
        return READER.getOrDefault(type, type + "FromText") + "('" + text + "')";
    }

    /** The registered names of the two families, as #registerAll registers them. */
    private static Set<String> familyNames() {
        Set<String> names = new TreeSet<>();
        for (String n : REGISTERED.keySet()) {
            if (FAMILY.matcher(n).matches()) {
                names.add(n);
            }
        }
        return names;
    }

    @Test
    void everyTopologicalNameIsRegistered() {
        List<String> missing = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : TOPOLOGICAL.entrySet()) {
            for (String op : e.getValue()) {
                if (!REGISTERED.containsKey(e.getKey() + op)) {
                    missing.add(e.getKey() + op);
                }
            }
        }
        assertTrue(missing.isEmpty(), "topological names not registered: " + missing);
    }

    @Test
    void everyOverloadAnswersUnquoted() throws Exception {
        Set<String> names = familyNames();
        int calls = 0;
        List<String> failures = new ArrayList<>();
        for (String name : names) {
            Matcher m = FAMILY.matcher(name);
            assertTrue(m.matches());
            boolean z = Z_OPS.contains(m.group(2));
            Class<?> fn = REGISTERED.get(name);
            for (Method eval : fn.getMethods()) {
                if (!eval.getName().equals("eval") || eval.getParameterCount() != 2) {
                    continue;
                }
                Class<?>[] p = eval.getParameterTypes();
                String sql = name + Arrays.toString(p);
                try {
                    sql = "SELECT " + name + "(" + arg(p[0], p[1], z) + ", "
                            + arg(p[1], p[0], z) + ")";
                    Object answer = scalar(sql);
                    if (!(answer instanceof Boolean)) {
                        failures.add(sql + " -> " + answer);
                    }
                } catch (Exception e) {
                    Throwable root = e;
                    while (root.getCause() != null) {
                        root = root.getCause();
                    }
                    failures.add(sql + " -> " + String.valueOf(root.getMessage()).split("\n")[0]);
                }
                calls++;
            }
        }
        assertTrue(calls > 0, "no overload of the two families is registered");
        assertTrue(failures.isEmpty(), failures.size() + " of " + calls
                + " overloads of " + names.size() + " names failed:\n"
                + String.join("\n", failures));
    }

    @Test
    void eachOperatorAnswersItsValue() throws Exception {
        // Two spans that overlap, the first ending before the second does
        String a = "floatspanFromText('[1, 3]')";
        String b = "floatspanFromText('[2, 4]')";
        Map<String, Boolean> span = new LinkedHashMap<>();
        span.put("spanOverlaps", true);
        span.put("spanContains", false);
        span.put("spanContained", false);
        span.put("spanAdjacent", false);
        span.put("spanLeft", false);
        span.put("spanOverleft", true);
        span.put("spanRight", false);
        span.put("spanOverright", false);
        for (Map.Entry<String, Boolean> e : span.entrySet()) {
            assertEquals(e.getValue(), scalar("SELECT " + e.getKey() + "(" + a + ", " + b + ")"),
                    e.getKey());
        }
        // A time span ending before the other starts
        String t1 = "tstzspanFromText('[2020-01-01, 2020-01-02]')";
        String t2 = "tstzspanFromText('[2020-01-03, 2020-01-04]')";
        assertEquals(true, scalar("SELECT spanBefore(" + t1 + ", " + t2 + ")"));
        assertEquals(true, scalar("SELECT spanOverbefore(" + t1 + ", " + t2 + ")"));
        assertEquals(false, scalar("SELECT spanAfter(" + t1 + ", " + t2 + ")"));
        assertEquals(false, scalar("SELECT spanOverafter(" + t1 + ", " + t2 + ")"));
        // A temporal Boolean whose time span is the time span it is compared with
        assertEquals(true, scalar("SELECT spanSame(" + t1
                + ", tboolFromText('[t@2020-01-01, f@2020-01-02]'))"));
        // A box left of, below, in front of and before the other
        String s1 = "stboxFromText('STBOX ZT(((1,1,1),(2,2,2)),[2020-01-01, 2020-01-02])')";
        String s2 = "stboxFromText('STBOX ZT(((3,3,3),(4,4,4)),[2020-01-03, 2020-01-04])')";
        Map<String, Boolean> box = new LinkedHashMap<>();
        box.put("stboxLeft", true);
        box.put("stboxRight", false);
        box.put("stboxBelow", true);
        box.put("stboxOverbelow", true);
        box.put("stboxAbove", false);
        box.put("stboxOverabove", false);
        box.put("stboxFront", true);
        box.put("stboxOverfront", true);
        box.put("stboxBack", false);
        box.put("stboxOverback", false);
        box.put("stboxBefore", true);
        box.put("stboxAfter", false);
        box.put("stboxOverlaps", false);
        box.put("stboxSame", false);
        for (Map.Entry<String, Boolean> e : box.entrySet()) {
            assertEquals(e.getValue(), scalar("SELECT " + e.getKey() + "(" + s1 + ", " + s2 + ")"),
                    e.getKey());
        }
    }
}
