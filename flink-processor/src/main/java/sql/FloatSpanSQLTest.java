package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import sql.types.floatspan.FloatSpanSerializer;
import sql.types.floatspan.FloatSpanTypeInfo;
import types.collections.number.FloatSpan;

import java.util.Arrays;

public class FloatSpanSQLTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = MobilityFlinkSQL.create(env);

        // Build a small in-memory stream of FloatSpan objects
        // constructed directly via JMEOS (floatspan_in)
        var spans = Arrays.asList(
                Row.of(1L, new FloatSpan("[1.0, 3.5]")),
                Row.of(2L, new FloatSpan("(2.0, 6.0)")),
                Row.of(3L, new FloatSpan("[5.0, 10.0]"))
        );

        var stream = env.fromCollection(
                spans,
                new RowTypeInfo(Types.LONG, FloatSpanTypeInfo.INSTANCE)
        );

        tEnv.createTemporaryView("spans", stream,
                org.apache.flink.table.api.Schema.newBuilder()
                        .column("f0", org.apache.flink.table.api.DataTypes.BIGINT())
                        // FloatSpan exposed as RAW type until catalog type support lands
                        .column("f1", org.apache.flink.table.api.DataTypes
                                .RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE))
                        .build());

        // This is the SQL users write — identical to MobilityDB syntax
        Table result = tEnv.sqlQuery("""
            SELECT
                f0                          AS id,
                floatspan_lower(f1)         AS `lower`,
                floatspan_upper(f1)         AS `upper`,
                floatspan_width(f1)         AS `width`,
                floatspan_contains(f1, CAST(2.5 AS FLOAT)) AS contains_2_5
            FROM spans
        """);

        result.execute().print();
    }
}