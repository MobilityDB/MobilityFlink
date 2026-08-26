package sql.udf.stbox;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxTypeInferenceSupport {

    public static TypeInference getTypeInference(DataTypeFactory f) {
        return stboxToBoolean();
    }

    static TypeInference stboxToBoolean() {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN().nullable()))
                .build();
    }

    public static TypeInference stboxTwoArgBoolean() {
        DataType stboxType = DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE);
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(stboxType),
                        InputTypeStrategies.explicit(stboxType)
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.BOOLEAN().nullable()))
                .build();
    }


}