package sql.udf.stbox;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import types.boxes.STBox;

public class STBoxHasZ extends ScalarFunction {

    public Boolean eval(STBox b) { return b == null ? null : b.has_z(); }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return STBoxTypeInferenceSupport.stboxToBoolean();
    }
}