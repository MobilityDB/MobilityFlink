package sql.udf.stbox;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.api.DataTypes;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxGetSpace extends ScalarFunction {

    public STBox eval(STBox s) {
        return s == null ? null : s.get_space();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE)
                ))
                .build();
    }
}