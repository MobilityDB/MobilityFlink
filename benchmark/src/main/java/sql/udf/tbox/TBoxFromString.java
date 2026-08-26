package sql.udf.tbox;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.api.DataTypes;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;

public class TBoxFromString extends ScalarFunction {

    public TBox eval(String s) {
        return s == null ? null : new TBox(s); // tbox_in via JMEOS
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.STRING())
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE)
                ))
                .build();
    }
}