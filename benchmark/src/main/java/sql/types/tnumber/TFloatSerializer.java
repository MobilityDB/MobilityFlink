package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import types.basic.tfloat.TFloat;
import types.basic.tfloat.TFloatInst;

public class TFloatSerializer extends TNumberSerializer<TFloat> {

    public static final TFloatSerializer INSTANCE = new TFloatSerializer();

    @Override
    protected String serialize(TFloat value) {
        return value.as_wkt(6); // tfloat_out via JMEOS — 6 decimal places
    }

    @Override
    protected TFloat deserialize(String text) {
        return new TFloatInst(text); // tfloat_in via JMEOS
    }

    @Override
    public TFloatSerializer duplicate() { return INSTANCE; }

    @Override
    public TFloat createInstance() { return null; }

    @Override
    public TypeSerializerSnapshot<TFloat> snapshotConfiguration() {
        return new TFloatSerializerSnapshot();
    }

    @Override public boolean equals(Object o)   { return o instanceof TFloatSerializer; }
    @Override public int hashCode()             { return TFloatSerializer.class.hashCode(); }
}