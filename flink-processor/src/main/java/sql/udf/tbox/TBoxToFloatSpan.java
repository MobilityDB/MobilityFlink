package sql.udf.tbox;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import sql.types.floatspan.FloatSpanSerializer;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;
import types.collections.number.FloatSpan;

public class TBoxToFloatSpan extends ScalarFunction {

    public FloatSpan eval(TBox b) {
        if (b == null) return null;
        try {
            return b.has_x() ? b.to_floatspan() : null;
        } catch (Exception e) {
            return null;  // time-only box → null instead of crash
        }
    }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE))))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(FloatSpan.class, FloatSpanSerializer.INSTANCE).nullable()))
                .build();
    }
}