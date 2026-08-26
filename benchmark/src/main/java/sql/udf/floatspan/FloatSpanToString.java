package sql.udf.floatspan;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.api.DataTypes;
import sql.types.floatspan.FloatSpanSerializer;
import types.collections.number.FloatSpan;

public class FloatSpanToString extends ScalarFunction {

    public String eval(FloatSpan s) {
        return s == null ? null : s.toString(6);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING().nullable()))
                .build();
    }
}