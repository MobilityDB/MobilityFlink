package sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.types.tnumber.TFloatSerializer;
import sql.types.tnumber.TFloatTypeInfo;
import sql.types.tnumber.TIntSerializer;
import sql.types.tnumber.TIntTypeInfo;
import types.basic.tfloat.TFloat;
import types.basic.tfloat.TFloatSeq;
import types.basic.tint.TInt;
import types.basic.tint.TIntSeq;

import java.util.Arrays;

public class TNumberSQLTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = MobilityFlinkSQL.create(env);

        // ── TFloat test data ──────────────────────────────────────────────
        // Interpolated float sequences representing sensor readings over time
        // Linear [] in order to support derivative() and round() functions
        var floats = Arrays.asList(
                Row.of(1L, new TFloatSeq("[1.5@2020-06-01, 3.0@2020-06-02, 2.5@2020-06-03]")),
                Row.of(2L, new TFloatSeq("[4.0@2020-06-01, 4.5@2020-06-02, 5.0@2020-06-03]")),
                Row.of(3L, new TFloatSeq("[10.9@2020-06-01, 7.2@2020-06-02, 3.8@2020-06-03]"))
        );

        var floatStream = env.fromCollection(
                floats,
                new RowTypeInfo(Types.LONG, TFloatTypeInfo.INSTANCE)
        );

        tEnv.createTemporaryView("tfloats", floatStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.RAW(TFloat.class, TFloatSerializer.INSTANCE))
                        .build());

        // ── TInt test data ────────────────────────────────────────────────
        var ints = Arrays.asList(
                Row.of(1L, new TIntSeq("{1@2020-06-01, 3@2020-06-02, 2@2020-06-03}")),
                Row.of(2L, new TIntSeq("{4@2020-06-01, 4@2020-06-02, 5@2020-06-03}")),
                Row.of(3L, new TIntSeq("{10@2020-06-01, 7@2020-06-02, 3@2020-06-03}"))
        );

        var intStream = env.fromCollection(
                ints,
                new RowTypeInfo(Types.LONG, TIntTypeInfo.INSTANCE)
        );

        tEnv.createTemporaryView("tints", intStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.RAW(TInt.class, TIntSerializer.INSTANCE))
                        .build());

        // ── Query 1: TFloat unary operations ─────────────────────────────
        System.out.println("\n=== TFloat: derivative, round, deltaValue ===");
        Table q1 = tEnv.sqlQuery("""
            SELECT
                f0                          AS id,
                tfloat_out(derivative(f1))  AS `derivative`,
                tfloat_out(tfloat_round(f1, 1)) AS `rounded_1dec`,
                tfloat_out(deltaValue(f1))  AS `delta_value`
            FROM tfloats
        """);
        q1.execute().print();

        // ── Query 2: TFloat arithmetic with scalar ────────────────────────
        System.out.println("\n=== TFloat: tAdd, tSub, with scalar ===");
        Table q2 = tEnv.sqlQuery("""
            SELECT
                f0                              AS id,
                tfloat_out(tAdd(f1, 2.0))       AS `plus_2`,
                tfloat_out(tSub(f1, 1.5))       AS `minus_1_5`
            FROM tfloats
        """);
        q2.execute().print();
        //tMul is left out because of a dependency problem linked to JMEOS and MEOS updates
        //tfloat_out(tMul(f1, 3.0))       AS `times_3`

        // ── Query 3: TFloat + TFloat ──────────────────────────────────────
        System.out.println("\n=== TFloat: tAdd(tfloat, tfloat) ===");
        Table q3 = tEnv.sqlQuery("""
            SELECT
                a.f0                                AS id,
                tfloat_out(tAdd(a.f1, b.f1))        AS `sum`
            FROM tfloats a
            JOIN tfloats b ON a.f0 = b.f0
            WHERE a.f0 = 1
        """);
        q3.execute().print();

        // ── Query 4: TInt arithmetic and deltaValue ───────────────────────
        System.out.println("\n=== TInt: tAdd, tSub, deltaValue ===");
        Table q4 = tEnv.sqlQuery("""
            SELECT
                f0                              AS id,
                tint_out(tAdd(f1, 10))          AS `plus_10`,
                tint_out(tSub(f1, 1))           AS `minus_1`,
                tint_out(deltaValue(f1))        AS `delta_value`
            FROM tints
        """);
        //tint_out(tMul(f1, 2))           AS `times_2`,
        q4.execute().print();
    }
}