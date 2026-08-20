package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import sql.types.tbox.TBoxSerializer;
import sql.types.tbox.TBoxTypeInfo;
import sql.udf.tbox.TBoxFromString;
import types.boxes.TBox;

import java.util.Arrays;

public class TBoxSQLTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = MobilityFlinkSQL.create(env);

        var boxes = Arrays.asList(
                Row.of(1L, new TBox("TBOXFLOAT XT([0, 10),[2020-06-01, 2020-06-05])")),
                Row.of(2L, new TBox("TBOXFLOAT XT([5, 20),[2020-06-03, 2020-06-10])")),
                Row.of(3L, new TBox("TBOX T([2020-06-01, 2020-06-05])"))   // time-only
        );

        var stream = env.fromCollection(
                boxes,
                new RowTypeInfo(Types.LONG, TBoxTypeInfo.INSTANCE)
        );

        tEnv.createTemporaryFunction("tbox", TBoxFromString.class);

        tEnv.createTemporaryView("tboxes", stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE))
                        .build());

        Table result = tEnv.sqlQuery("""
            SELECT
                f0                                          AS id,
                tbox_has_x(f1)                               AS `has_x`,
                tbox_has_t(f1)                               AS `has_t`,
            floatspan_out(tbox_to_floatspan(f1))            AS `float_span`,
                tbox_overlaps(f1,
                    tbox('TBOXFLOAT XT([8, 15),[2020-06-04, 2020-06-08])'))
                                                            AS `overlaps_query`
            FROM tboxes
        """);

        result.execute().print();

        // ── Query : aggregation ───────────────────────────────────────
        System.out.println("\n=== TBox: Extent ===");
        Table query = tEnv.sqlQuery("""
            SELECT
                floatspan_out(tbox_to_floatspan(tbox_extent(f1))) AS `extent`
            FROM tboxes
            WHERE tbox_has_x(f1) = true AND tbox_has_t(f1) = true
        """);
        query.execute().print();
    }
}