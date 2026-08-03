package sql.udf.stbox;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import types.boxes.STBox;

public class STBoxOverlaps extends ScalarFunction {

    public Boolean eval(STBox a, STBox b) {
        return a == null || b == null ? null : a.overlaps(b);
    }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return STBoxTypeInferenceSupport.stboxTwoArgBoolean();
    }
}