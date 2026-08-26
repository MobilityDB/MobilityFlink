// TBoxIntersection.java — tbox_intersection(tbox, tbox)
package sql.udf.tbox;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import types.boxes.TBox;

public class TBoxIntersection extends ScalarFunction {
    public TBox eval(TBox a, TBox b) {
        return a == null || b == null ? null : a.intersection(b);
    }
    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TBoxTypeInferenceSupport.tboxTwoArgTBox();
    }
}