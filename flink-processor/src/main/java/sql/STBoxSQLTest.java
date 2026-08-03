package sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.types.stbox.STBoxSerializer;
import sql.types.stbox.STBoxTypeInfo;
import types.boxes.STBox;

import java.util.Arrays;

public class STBoxSQLTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = MobilityFlinkSQL.create(env);

        var boxes = Arrays.asList(
                Row.of(1L, new STBox("STBOX XT(((1.5,2.5),(3.3,4.4)),[2020-06-01,2020-06-05])")),
                Row.of(2L, new STBox("STBOX XT(((5.5,6.5),(8.5,9.5)),[2020-06-03,2020-06-10])")),
                Row.of(3L, new STBox("STBOX X((0.5,0.5),(10.5,10.5))")),  // space-only
                Row.of(4L, new STBox("STBOX T([2020-06-01,2020-06-05])"))   // time-only
        );

        var stream = env.fromCollection(
                boxes,
                new RowTypeInfo(Types.LONG, STBoxTypeInfo.INSTANCE)
        );

        tEnv.createTemporaryView("stboxes", stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))
                        .build());

        // ── Query 1: accesseurs ───────────────────────────────────────────
        System.out.println("\n=== STBox: accesseurs has_xy, has_t, xMin, yMin ===");
        Table q1 = tEnv.sqlQuery("""
            SELECT
                f0                      AS id,
                stbox_has_xy(f1)        AS `has_xy`,
                stbox_has_t(f1)         AS `has_t`,
                stbox_xmin(f1)          AS `x_min`,
                stbox_ymin(f1)          AS `y_min`
            FROM stboxes
            WHERE stbox_has_xy(f1) = true
        """);
        q1.execute().print();

        // ── Query 2: topologie ────────────────────────────────────────────
        System.out.println("\n=== STBox: overlaps, contains ===");
        Table q2 = tEnv.sqlQuery("""
            SELECT
                f0                          AS id,
                stbox_overlaps(f1,
                    stbox('STBOX XT(((2.0,3.0),(6.0,7.0)),[2020-06-02,2020-06-06])')) AS `overlaps_query`,
                stbox_contains(f1,
                    stbox('STBOX X((1.5,2.5),(2.5,3.5))')) AS `contains_small`
            FROM stboxes
            WHERE stbox_has_xy(f1) = true
        """);
        q2.execute().print();

        // ── Query 3: transformation ───────────────────────────────────────
        System.out.println("\n=== STBox: expandSpace, getSpace ===");
        Table q3 = tEnv.sqlQuery("""
            SELECT
                f0                          AS id,
                stbox_out(stbox_expand(f1, CAST(1.0 AS FLOAT))) AS `expanded`,
                stbox_out(stbox_get_space(f1)) AS `space_only`
            FROM stboxes
            WHERE stbox_has_xy(f1) = true
        """);
        q3.execute().print();

    }
}