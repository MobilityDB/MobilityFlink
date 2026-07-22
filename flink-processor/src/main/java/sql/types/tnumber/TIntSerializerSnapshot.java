package sql.types.tnumber;

import types.basic.tint.TInt;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class TIntSerializerSnapshot
        extends TNumberSerializerSnapshot<TInt> {

    @Override
    public TypeSerializer<TInt> restoreSerializer() {
        return TIntSerializer.INSTANCE;
    }

    @Override
    protected Class<TIntSerializer> serializerClass() {
        return TIntSerializer.class;
    }
}