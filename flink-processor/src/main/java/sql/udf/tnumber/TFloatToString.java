package sql.udf.tnumber;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.api.DataTypes;
import sql.types.tnumber.TFloatSerializer;
import types.basic.tfloat.TFloat;

public class TFloatToString extends ScalarFunction {

    public String eval(TFloat t) {
        return t == null ? null : t.as_wkt(6);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(TFloat.class, TFloatSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING().nullable()))
                .build();
    }
}