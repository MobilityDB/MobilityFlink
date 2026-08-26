package sql.udf.tbox;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import types.boxes.TBox;

public class TBoxIsOverBefore extends ScalarFunction {

    public Boolean eval(TBox a, TBox b) {
        return a == null || b == null ? null : a.is_over_or_before(b);
    }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TBoxTypeInferenceSupport.tboxTwoArgBoolean();
    }
}