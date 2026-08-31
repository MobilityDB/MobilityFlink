package sql.udf.stbox;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxExtent
        extends AggregateFunction<STBox, STBoxExtent.Accumulator> {

    public static class Accumulator {
        public String currentSpanWkt = null;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, STBox box) {
        if (box == null) return;
        if (acc.currentSpanWkt == null) {
            acc.currentSpanWkt = box.toString(6);
            return;
        }
        // rebuild current box from WKT
        STBox current = new STBox(acc.currentSpanWkt);
        Pointer result = GeneratedFunctions.union_stbox_stbox(
                current.get_inner(),
                box.get_inner(),
                false
        );
        acc.currentSpanWkt = new STBox(result).toString(6);
    }

    @Override
    public STBox getValue(Accumulator acc) {
        if (acc.currentSpanWkt == null) return null;
        return new STBox(acc.currentSpanWkt);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE))
                ))
                .accumulatorTypeStrategy(TypeStrategies.explicit(
                        DataTypes.STRUCTURED(
                                Accumulator.class,
                                DataTypes.FIELD("currentSpanWkt", DataTypes.STRING().nullable())
                        )
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(STBox.class, STBoxSerializer.INSTANCE)
                ))
                .build();
    }
}