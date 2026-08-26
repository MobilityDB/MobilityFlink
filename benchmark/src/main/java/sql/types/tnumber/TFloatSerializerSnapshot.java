package sql.types.tnumber;

import types.basic.tfloat.TFloat;

public class TFloatSerializerSnapshot
        extends TNumberSerializerSnapshot<TFloat> {

    public TFloatSerializerSnapshot() {
        super(() -> TFloatSerializer.INSTANCE);
    }
}