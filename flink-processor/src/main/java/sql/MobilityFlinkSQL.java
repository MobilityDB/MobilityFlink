package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.udf.floatspan.*;
import sql.udf.stbox.*;
import sql.udf.tbox.*;
import sql.udf.tnumber.*;

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
        tEnv.createTemporaryFunction("floatspan_out", FloatSpanToString.class);

        // TBox UDFs
        tEnv.createTemporaryFunction("tbox_hasx",          TBoxHasX.class);
        tEnv.createTemporaryFunction("tbox_hast",          TBoxHasT.class);
        tEnv.createTemporaryFunction("tbox_to_floatspan",  TBoxToFloatSpan.class);
        tEnv.createTemporaryFunction("tbox_contains",      TBoxContains.class);
        tEnv.createTemporaryFunction("tbox_contained",     TBoxContainedIn.class);
        tEnv.createTemporaryFunction("tbox_overlaps",      TBoxOverlaps.class);
        tEnv.createTemporaryFunction("tbox_same",          TBoxSame.class);
        tEnv.createTemporaryFunction("tbox_adjacent",      TBoxAdjacent.class);
        tEnv.createTemporaryFunction("tbox_left",          TBoxIsLeft.class);
        tEnv.createTemporaryFunction("tbox_overleft",      TBoxIsOverLeft.class);
        tEnv.createTemporaryFunction("tbox_right",         TBoxIsRight.class);
        tEnv.createTemporaryFunction("tbox_overright",     TBoxIsOverRight.class);
        tEnv.createTemporaryFunction("tbox_before",        TBoxIsBefore.class);
        tEnv.createTemporaryFunction("tbox_overbefore",    TBoxIsOverBefore.class);
        tEnv.createTemporaryFunction("tbox_after",         TBoxIsAfter.class);
        tEnv.createTemporaryFunction("tbox_overafter",     TBoxIsOverAfter.class);
        tEnv.createTemporaryFunction("tbox_union",         TBoxUnion.class);
        tEnv.createTemporaryFunction("tbox_intersection",  TBoxIntersection.class);

        // TNumber UDFs
        tEnv.createTemporaryFunction("tAdd",        TNumberAdd.class);
        tEnv.createTemporaryFunction("tSub",        TNumberSub.class);
        tEnv.createTemporaryFunction("tMul",        TNumberMul.class);
        tEnv.createTemporaryFunction("deltaValue",  TNumberDeltaValue.class);
        tEnv.createTemporaryFunction("derivative",  TFloatDerivative.class);
        tEnv.createTemporaryFunction("tfloat_round", TFloatRound.class);
        tEnv.createTemporaryFunction("tfloat_out",  TFloatToString.class);
        tEnv.createTemporaryFunction("tint_out",    TIntToString.class);

        // STBox UDFs
        tEnv.createTemporaryFunction("stbox",           STBoxFromString.class);
        tEnv.createTemporaryFunction("stbox_has_xy",    STBoxHasXY.class);
        tEnv.createTemporaryFunction("stbox_has_t",     STBoxHasT.class);
        tEnv.createTemporaryFunction("stbox_xmin",      STBoxXMin.class);
        tEnv.createTemporaryFunction("stbox_ymin",      STBoxYMin.class);
        tEnv.createTemporaryFunction("stbox_overlaps",  STBoxOverlaps.class);
        tEnv.createTemporaryFunction("stbox_contains",  STBoxContains.class);
        tEnv.createTemporaryFunction("stbox_expand",    STBoxExpandSpace.class);
        tEnv.createTemporaryFunction("stbox_get_space", STBoxGetSpace.class);
        tEnv.createTemporaryFunction("stbox_out",       STBoxToString.class);

        return tEnv;
    }
}