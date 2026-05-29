package org.mobilitydb.flink.meos;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Runtime check that the cbuffer facade family calls into libmeos and returns
 * correct results. Compiled and run only when the build includes the cbuffer
 * family ({@code -DCBUFFER=ON}); the family requires a libmeos built with
 * {@code -DCBUFFER=ON}.
 */
@EnabledIfSystemProperty(named = "mobilityflink.meos.enabled", matches = "true")
class MeosCbufferSmokeTest {

    @BeforeAll
    static void init() {
        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();
    }

    @AfterAll
    static void finalizeMeos() {
        GeneratedFunctions.meos_finalize();
    }

    @Test
    void cbuffer() {
        Pointer cb = MeosOpsFreeCbuffer.cbuffer_make(MeosOpsFreeGeo.geom_in("POINT(1 1)", 0), 0.5);
        assertNotNull(cb);
        assertEquals(0.5, MeosOpsFreeCbuffer.cbuffer_radius(cb), 1e-9);
        assertNotNull(MeosOpsFreeCbuffer.cbuffer_out(cb, 6));
    }
}
