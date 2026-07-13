package sql.udf.floatspan;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.DataType;
import sql.types.FloatSpanSerializer;
import types.collections.number.FloatSpan;

public class FloatSpanLower extends ScalarFunction {

    public Float eval(FloatSpan s) {
        return s == null ? null : s.lower();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        DataType floatSpanType = DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE);
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(floatSpanType)
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.FLOAT().nullable()
                ))
                .build();
    }
}