package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import types.basic.tint.TInt;
import types.basic.tint.TIntInst;

public class TIntSerializer extends TNumberSerializer<TInt> {

    public static final TIntSerializer INSTANCE = new TIntSerializer();

    @Override
    protected String serialize(TInt value) {
        return value.as_wkt(); // tint_out via JMEOS
    }

    @Override
    protected TInt deserialize(String text) {
        return new TIntInst(text); // tint_in via JMEOS
    }

    @Override
    public TIntSerializer duplicate() { return INSTANCE; }

    @Override
    public TInt createInstance() { return null; }

    @Override
    public TypeSerializerSnapshot<TInt> snapshotConfiguration() {
        return new TIntSerializerSnapshot();
    }

    @Override public boolean equals(Object o)   { return o instanceof TIntSerializer; }
    @Override public int hashCode()             { return TIntSerializer.class.hashCode(); }
}