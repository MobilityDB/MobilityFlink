package sql.udf.tnumber;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import types.basic.tfloat.TFloat;

public class TFloatDerivative extends ScalarFunction {

    public TFloat eval(TFloat t) {
        if (t == null) return null;
        return t.derivative(); // Temporal_derivative via JMEOS
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                TNumberTypeInferenceSupport.TFLOAT_TYPE)
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        TNumberTypeInferenceSupport.TFLOAT_TYPE
                ))
                .build();
    }
}