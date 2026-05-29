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
 * Runtime check that the npoint facade family calls into libmeos and returns
 * correct results. Compiled and run when the build includes the npoint family
 * (the default; dropped with {@code -DNPOINT=OFF}).
 */
@EnabledIfSystemProperty(named = "mobilityflink.meos.enabled", matches = "true")
class MeosNpointSmokeTest {

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
    void npoint() {
        Pointer np = MeosOpsFreeNpoint.npoint_make(1, 0.5);
        assertNotNull(np);
        assertEquals(1, MeosOpsFreeNpoint.npoint_route(np));
        assertEquals(0.5, MeosOpsFreeNpoint.npoint_position(np), 1e-9);
    }
}
