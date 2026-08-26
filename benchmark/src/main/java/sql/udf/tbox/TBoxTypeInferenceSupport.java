package sql.udf.tbox;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;

public class TBoxTypeInferenceSupport {
    static TypeInference tboxToBoolean() {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE))))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN().nullable()))
                .build();
    }

    static TypeInference tboxTwoArgBoolean() {
        var tboxType = DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE);
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(tboxType),
                        InputTypeStrategies.explicit(tboxType)))
                .outputTypeStrategy(TypeStrategies.explicit(DataTypes.BOOLEAN().nullable()))
                .build();
    }

    static TypeInference tboxTwoArgTBox() {
        var tboxType = DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE);
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(tboxType),
                        InputTypeStrategies.explicit(tboxType)))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE).nullable()))
                .build();
    }
}
