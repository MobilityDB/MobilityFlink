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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The SQL surface in a table environment that runs `LOAD MODULE mobilitydb` and never calls
 * MobilityFlinkSql.registerAll, as a SQL Client or SQL Gateway session runs plain SQL: Flink's
 * service loader finds the module's factory, the module holds every function registerAll
 * registers, and its functions plan to the same types and answer the same values, alone and
 * beside registerAll. The twin of MobilitySpark's MobilitySparkExtensionsTest.
 */
class MobilityDBModuleTest {

    private static final String INSTANT =
            "tfloat(CAST(1.5 AS DOUBLE), TO_TIMESTAMP_LTZ(1577836800000, 3))";

    private static final String TEXT = "1.5@2020-01-01 00:00:00+00";

    private static TableEnvironment loaded() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql("LOAD MODULE mobilitydb");
        return env;
    }

    private static TableEnvironment registered() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        MobilityFlinkSql.registerAll(env);
        return env;
    }

    /**
     * The first field of every row the query emits, read as GeneratedSqlSurfaceTest#rows reads
     * the rows.
     */
    private static List<Object> column(TableEnvironment env, String sql) throws Exception {
        List<Object> out = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(sql).collect()) {
            it.forEachRemaining(r -> out.add(r.getField(0)));
        }
        return out;
    }

    private static Set<String> lowerCase(Iterable<String> names) {
        Set<String> out = new TreeSet<>();
        names.forEach(n -> out.add(n.toLowerCase(Locale.ROOT)));
        return out;
    }

    @Test
    void theLoadedModuleAnswersWithoutRegisterAll() throws Exception {
        TableEnvironment env = loaded();
        assertEquals(List.of(TEXT), column(env, "SELECT asText(" + INSTANT + ")"));
        assertEquals(List.of(TEXT), column(env, "SELECT asText(tfloatFromText('" + TEXT + "'))"));
    }

    @Test
    void showModulesListsTheModuleAfterCore() throws Exception {
        assertEquals(List.of("core", "mobilitydb"), column(loaded(), "SHOW MODULES"));
    }

    @Test
    void theModuleHoldsEveryFunctionRegisterAllRegisters() {
        assertEquals(lowerCase(Arrays.asList(registered().listUserDefinedFunctions())),
                lowerCase(MobilityDBModule.INSTANCE.listFunctions()));
    }

    @Test
    void aModuleFunctionPlansToTheTypeRegisterAllGives() {
        String sql = "SELECT " + INSTANT + ", asText(" + INSTANT + ")";
        assertEquals(registered().sqlQuery(sql).getResolvedSchema().getColumnDataTypes(),
                loaded().sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    }

    @Test
    void aBuiltinNameAnswersFromTheModuleListedBeforeCore() throws Exception {
        // the module twin of GeneratedSqlSurfaceTest#aBuiltinNameResolvesToTheBuiltinUnlessQualified
        TableEnvironment env = loaded();
        assertThrows(Exception.class, () -> column(env, "SELECT lower(floatspan_in('[1, 3]'))"));
        env.executeSql("USE MODULES mobilitydb, core");
        assertEquals(List.of(1.0), column(env, "SELECT lower(floatspan_in('[1, 3]'))"));
    }

    @Test
    void registerAllBesideTheModuleAnswersAlike() throws Exception {
        TableEnvironment env = loaded();
        MobilityFlinkSql.registerAll(env);
        assertEquals(List.of(TEXT), column(env, "SELECT asText(" + INSTANT + ")"));
    }
}
