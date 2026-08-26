package sql.udf.stbox;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import sql.types.stbox.STBoxSerializer;
import types.boxes.STBox;

public class STBoxHasXY extends ScalarFunction {

    public Boolean eval(STBox b) { return b == null ? null : b.has_xy(); }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return STBoxTypeInferenceSupport.stboxToBoolean();
    }
}