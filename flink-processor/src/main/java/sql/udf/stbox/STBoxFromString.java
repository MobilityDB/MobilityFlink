package sql.udf.stbox;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.api.DataTypes;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxFromString extends ScalarFunction {

    public STBox eval(String s) {
        return s == null ? null : new STBox(s); // STBox_in via JMEOS
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.STRING())
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE)
                ))
                .build();
    }
}