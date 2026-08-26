package sql.types.tnumber;

import types.basic.tint.TInt;

public class TIntSerializerSnapshot
        extends TNumberSerializerSnapshot<TInt> {

    public TIntSerializerSnapshot() {
        super(() -> TIntSerializer.INSTANCE);
    }
}