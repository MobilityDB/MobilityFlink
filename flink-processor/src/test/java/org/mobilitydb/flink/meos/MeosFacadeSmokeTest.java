package org.mobilitydb.flink.meos;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime check that the always-built MEOS facade families (core and geo) call
 * into libmeos and return correct results. Each constructs a value through a
 * {@code MeosOps*} facade method and reads it back. Runs only with
 * {@code -Dmobilityflink.meos.enabled=true} and a libmeos on the load path. The
 * optional families have their own gated smoke tests
 * ({@link MeosCbufferSmokeTest}, {@link MeosNpointSmokeTest},
 * {@link MeosPoseSmokeTest}), each compiled only when its build flag includes
 * the family.
 */
@EnabledIfSystemProperty(named = "mobilityflink.meos.enabled", matches = "true")
class MeosFacadeSmokeTest {

    @BeforeAll
    static void init() {
        // No-op error handler so a parse error returns rather than terminating the JVM.
        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();
    }

    @AfterAll
    static void finalizeMeos() {
        GeneratedFunctions.meos_finalize();
    }

    @Test
    void coreTbox() {
        Pointer tbox = MeosOpsTBox.tbox_in("TBOX X([1, 2])");
        assertNotNull(tbox);
        assertTrue(MeosOpsTBox.tbox_out(tbox, 6).contains("TBOX"));
    }

    @Test
    void coreIntspan() {
        Pointer span = MeosOpsIntSpan.intspan_in("[1, 5)");
        assertNotNull(span);
        String out = MeosOpsIntSpan.intspan_out(span);
        assertTrue(out.contains("1") && out.contains("5"));
    }

    @Test
    void geoStbox() {
        Pointer stbox = MeosOpsSTBox.stbox_in("STBOX X((1,1),(2,2))");
        assertNotNull(stbox);
        assertTrue(MeosOpsSTBox.stbox_out(stbox, 6).contains("STBOX"));
    }

    @Test
    void geoGeometry() {
        Pointer geom = MeosOpsFreeGeo.geom_in("POINT(1 1)", 0);
        assertNotNull(geom);
        assertTrue(MeosOpsFreeGeo.geo_as_text(geom, 6).toUpperCase().contains("POINT"));
    }
}
