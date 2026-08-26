package sql.udf.stbox;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxExpandSpace extends ScalarFunction {

    // stbox + integer → stbox
    public STBox eval(STBox a, Integer distance) throws Exception {
        return a == null || distance == null ? null : a.expand_numerical(distance);
    }

    // stbox + float → stbox
    public STBox eval(STBox a, Float distance) throws Exception {
        return a == null || distance == null ? null : a.expand_numerical(distance);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        DataType stboxType = DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE);

        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.or(
                        // stbox + integer
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(stboxType),
                                InputTypeStrategies.explicit(DataTypes.INT())
                        ),
                        // stbox + float
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.explicit(stboxType),
                                InputTypeStrategies.explicit(DataTypes.FLOAT())
                        )
                ))
                .outputTypeStrategy(TypeStrategies.explicit(stboxType.nullable()))
                .build();
    }
}