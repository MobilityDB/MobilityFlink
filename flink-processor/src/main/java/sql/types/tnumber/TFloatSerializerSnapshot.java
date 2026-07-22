package sql.types.tnumber;

import types.basic.tfloat.TFloat;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class TFloatSerializerSnapshot
        extends TNumberSerializerSnapshot<TFloat> {

    @Override
    public TypeSerializer<TFloat> restoreSerializer() {
        return TFloatSerializer.INSTANCE;
    }

    @Override
    protected Class<TFloatSerializer> serializerClass() {
        return TFloatSerializer.class;
    }
}