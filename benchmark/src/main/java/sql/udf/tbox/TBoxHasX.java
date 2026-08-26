package sql.udf.tbox;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;

public class TBoxHasX extends ScalarFunction {

    public Boolean eval(TBox b) { return b == null ? null : b.has_x(); }

    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TBoxTypeInferenceSupport.tboxToBoolean();
    }


}