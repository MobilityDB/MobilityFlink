package sql.udf.tbox;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;

public class TBoxExtent
        extends AggregateFunction<TBox, TBoxExtent.Accumulator> {

    public static class Accumulator {
        public String currentSpanWkt = null;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, TBox box) {
        if (box == null) return;
        if (acc.currentSpanWkt == null) {
            acc.currentSpanWkt = box.toString(6);
            return;
        }
        // rebuild current box from WKT
        TBox current = new TBox(acc.currentSpanWkt);
        Pointer result = GeneratedFunctions.union_tbox_tbox(
                current.get_inner(),
                box.get_inner(),
                false
        );
        acc.currentSpanWkt = new TBox(result).toString(6);
    }

    @Override
    public TBox getValue(Accumulator acc) {
        if (acc.currentSpanWkt == null) return null;
        return new TBox(acc.currentSpanWkt);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(
                                DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE))
                ))
                .accumulatorTypeStrategy(TypeStrategies.explicit(
                        DataTypes.STRUCTURED(
                                Accumulator.class,
                                DataTypes.FIELD("currentSpanWkt", DataTypes.STRING().nullable())
                        )
                ))
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.RAW(TBox.class, TBoxSerializer.INSTANCE)
                ))
                .build();
    }
}