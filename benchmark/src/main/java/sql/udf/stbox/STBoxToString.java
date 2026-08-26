package sql.udf.stbox;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.api.DataTypes;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxToString extends ScalarFunction {

    public String eval(STBox t) {
        return t == null ? null : t.toString(6);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING().nullable()))
                .build();
    }
}