package sql.udf.floatspan;

import functions.functions;
import jnr.ffi.Pointer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.floatspan.FloatSpanSerializer;
import types.collections.number.FloatSpan;

public class FloatSpanExtent
        extends AggregateFunction<FloatSpan, FloatSpanExtent.Accumulator> {

    public static class Accumulator {
        public String currentSpanWkt = null; // WKT serialized state
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, FloatSpan span) {
        if (span == null) return;
        if (acc.currentSpanWkt == null) {
            acc.currentSpanWkt = span.toString(6);
            return;
        }
        // rebuild current span from WKT
        FloatSpan current = new FloatSpan(acc.currentSpanWkt);
        Pointer result = functions.span_extent_transfn(
                current.get_inner(),
                span.get_inner()
        );
        acc.currentSpanWkt = new FloatSpan(result).toString(6);
    }

    @Override
    public FloatSpan getValue(Accumulator acc) {
        if (acc.currentSpanWkt == null) return null;
        return new FloatSpan(acc.currentSpanWkt);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE))
                ))
                .accumulatorTypeStrategy(TypeStrategies.explicit(
                        DataTypes.STRUCTURED(
                                Accumulator.class,
                                DataTypes.FIELD("currentSpanWkt", DataTypes.STRING().nullable())
                        )
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE)
                ))
                .build();
    }
}