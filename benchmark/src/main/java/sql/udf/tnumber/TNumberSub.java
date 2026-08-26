package sql.udf.tnumber;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import types.basic.tnumber.TNumber;

public class TNumberSub extends ScalarFunction {

    // tint + integer → tint
    public TNumber eval(TNumber t, Integer i) throws Exception {
        return t == null || i == null ? null : t.sub(i);
    }

    // tfloat + float → tfloat
    public TNumber eval(TNumber t, Float f) throws Exception {
        return t == null || f == null ? null : t.sub(f);
    }

    // tint + tint → tint  /  tfloat + tfloat → tfloat
    public TNumber eval(TNumber t, TNumber t2) throws Exception {
        return t == null || t2 == null ? null : t.sub(t2);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.or(
                        // tint + integer
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TINT_TYPE),
                                InputTypeStrategies.explicit(DataTypes.INT())
                        ),
                        // tfloat + float
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TFLOAT_TYPE),
                                InputTypeStrategies.explicit(DataTypes.FLOAT())
                        ),
                        // tint + tint
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TINT_TYPE),
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TINT_TYPE)
                        ),
                        // tfloat + tfloat
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TFLOAT_TYPE),
                                InputTypeStrategies.explicit(TNumberTypeInferenceSupport.TFLOAT_TYPE)
                        )
                ))
                .outputTypeStrategy(callContext -> {
                    if (callContext.getArgumentDataTypes().get(0)
                            .equals(TNumberTypeInferenceSupport.TINT_TYPE)) {
                        return java.util.Optional.of(TNumberTypeInferenceSupport.TINT_TYPE);
                    }
                    return java.util.Optional.of(TNumberTypeInferenceSupport.TFLOAT_TYPE);
                })
                .build();
    }
}