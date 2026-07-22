package sql.udf.tnumber;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import sql.types.tnumber.TFloatSerializer;
import sql.types.tnumber.TIntSerializer;
import types.basic.tfloat.TFloat;
import types.basic.tint.TInt;

public final class TNumberTypeInferenceSupport {

    private TNumberTypeInferenceSupport() {}

    public static final DataType TFLOAT_TYPE =
            DataTypes.RAW(TFloat.class, TFloatSerializer.INSTANCE);

    public static final DataType TINT_TYPE =
            DataTypes.RAW(TInt.class, TIntSerializer.INSTANCE);
}