package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.basic.tfloat.TFloat;

public class TFloatTypeInfo extends TNumberTypeInfo<TFloat> {

    public static final TFloatTypeInfo INSTANCE = new TFloatTypeInfo();

    @Override
    public Class<TFloat> getTypeClass() { return TFloat.class; }

    @Override
    protected TypeSerializer<TFloat> serializer() {
        return TFloatSerializer.INSTANCE;
    }
}