package sql.udf.tnumber;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.api.DataTypes;
import sql.types.tnumber.TIntSerializer;
import types.basic.tint.TInt;

public class TIntToString extends ScalarFunction {

    public String eval(TInt t) {
        return t == null ? null : t.as_wkt();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(TInt.class, TIntSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING().nullable()))
                .build();
    }
}