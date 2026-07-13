package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.udf.floatspan.*;

public class MobilityFlinkSQL {

    public static StreamTableEnvironment create(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // FloatSpan UDFs — MobilityDB SQL naming
        tEnv.createTemporaryFunction("floatspan_lower",    FloatSpanLower.class);
        tEnv.createTemporaryFunction("floatspan_upper",    FloatSpanUpper.class);
        tEnv.createTemporaryFunction("floatspan_width",    FloatSpanWidth.class);
        tEnv.createTemporaryFunction("floatspan_contains", FloatSpanContains.class);
        tEnv.createTemporaryFunction("floatspan_overlaps", FloatSpanOverlaps.class);
        tEnv.createTemporaryFunction("floatspan_distance", FloatSpanDistance.class);

        return tEnv;
    }
}