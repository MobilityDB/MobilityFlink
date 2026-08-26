package sql.udf.stbox;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxXMin extends ScalarFunction {

    public Float eval(STBox s) {
        if (s == null) return null;
        try {
            return s.has_xy() ? s.xmin() : null;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.FLOAT().nullable()
                ))
                .build();
    }
}
