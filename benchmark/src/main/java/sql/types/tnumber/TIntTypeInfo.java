package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.basic.tint.TInt;

public class TIntTypeInfo extends TNumberTypeInfo<TInt> {

    public static final TIntTypeInfo INSTANCE = new TIntTypeInfo();

    @Override
    public Class<TInt> getTypeClass() { return TInt.class; }

    @Override
    protected TypeSerializer<TInt> serializer() {
        return TIntSerializer.INSTANCE;
    }
}