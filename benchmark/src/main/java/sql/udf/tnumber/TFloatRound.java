package sql.udf.tnumber;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import types.basic.tfloat.TFloat;

public class TFloatRound extends ScalarFunction {

    // round(tfloat, integer)
    public TFloat eval(TFloat t, Integer decimals) throws Exception {
        if (t == null) return null;
        return (TFloat) t.round(decimals == null ? 0 : decimals);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                TNumberTypeInferenceSupport.TFLOAT_TYPE),
                        InputTypeStrategies.explicit(
                                DataTypes.INT().nullable())
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        TNumberTypeInferenceSupport.TFLOAT_TYPE
                ))
                .build();
    }
}