package sql.udf.tnumber;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import types.basic.tfloat.TFloat;
import types.basic.tint.TInt;

public class TNumberDeltaValue extends ScalarFunction {

    // tint overload
    public TInt eval(TInt t) {
        if (t == null) return null;
        return (TInt) t.delta_value(); // Tnumber_delta_value via JMEOS
    }

    // tfloat overload
    public TFloat eval(TFloat t) {
        if (t == null) return null;
        return (TFloat) t.delta_value(); // Tnumber_delta_value via JMEOS
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.or(
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(
                                        TNumberTypeInferenceSupport.TINT_TYPE)
                        ),
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(
                                        TNumberTypeInferenceSupport.TFLOAT_TYPE)
                        )
                ))
                .outputTypeStrategy(callContext -> {
                    // output type mirrors input type
                    if (callContext.getArgumentDataTypes().get(0)
                            .equals(TNumberTypeInferenceSupport.TINT_TYPE)) {
                        return java.util.Optional.of(
                                TNumberTypeInferenceSupport.TINT_TYPE);
                    }
                    return java.util.Optional.of(
                            TNumberTypeInferenceSupport.TFLOAT_TYPE);
                })
                .build();
    }
}