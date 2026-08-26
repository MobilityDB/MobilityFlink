package sql.udf.floatspan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.floatspan.FloatSpanSerializer;
import types.collections.number.FloatSpan;

public class FloatSpanContains extends ScalarFunction {
    public Boolean eval( FloatSpan s, FloatSpan other) throws Exception {
        return s == null || other == null ? null : s.contains(other);
    }
    public Boolean eval(FloatSpan s, Float value) throws Exception {
        return s == null || value == null ? null : s.contains(value);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        DataType floatSpanType = DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE);
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.or(
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(floatSpanType),
                                InputTypeStrategies.explicit(floatSpanType)
                        ),
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(floatSpanType),
                                InputTypeStrategies.explicit(DataTypes.FLOAT())
                        )
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.BOOLEAN().nullable()
                ))
                .build();
    }
}