package sql.udf.tbox;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import types.boxes.TBox;

public class TBoxHasT extends ScalarFunction {

    public Boolean eval(TBox b) { return b == null ? null : b.has_t(); }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TBoxTypeInferenceSupport.tboxToBoolean();
    }
}