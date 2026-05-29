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
 * Runtime check that the pose facade family calls into libmeos and returns
 * correct results. Compiled and run only when the build includes the pose
 * family ({@code -DPOSE=ON}); the family requires a libmeos built with
 * {@code -DPOSE=ON}.
 */
@EnabledIfSystemProperty(named = "mobilityflink.meos.enabled", matches = "true")
class MeosPoseSmokeTest {

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
    void pose() {
        Pointer pose = MeosOpsFreePose.pose_in("Pose(Point(1 1), 0.5)");
        assertNotNull(pose);
        assertNotNull(MeosOpsFreePose.pose_out(pose, 6));
        assertEquals(0.5, MeosOpsFreePose.pose_rotation(pose), 1e-9);
    }
}
