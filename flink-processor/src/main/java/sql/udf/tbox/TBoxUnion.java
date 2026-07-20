package sql.udf.tbox;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.*;
import sql.types.tbox.TBoxSerializer;
import types.boxes.TBox;

public class TBoxUnion extends ScalarFunction {
    public TBox eval(TBox a, TBox b) {
        return a == null || b == null ? null : a.union(b, true);
    }
    @Override public TypeInference getTypeInference(DataTypeFactory f) {
        return TBoxHasX.tboxTwoArgTBox();
    }
}