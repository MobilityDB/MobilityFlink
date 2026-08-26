package sql.types.tbox;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.boxes.TBox;

import java.io.IOException;

public class TBoxSerializer extends TypeSerializer<TBox> {

    public static final TBoxSerializer INSTANCE = new TBoxSerializer();

    @Override
    public void serialize(TBox value, DataOutputView target) throws IOException {
        target.writeUTF(value.toString()); // tbox_out
    }

    @Override
    public TBox deserialize(DataInputView source) throws IOException {
        return new TBox(source.readUTF()); // tbox_in
    }

    @Override
    public TBox deserialize(TBox reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public TBox copy(TBox from) {
        return new TBox(from.toString()); // round-trip, never copy pointer
    }

    @Override
    public TBox copy(TBox from, TBox reuse) { return copy(from); }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
    }

    @Override public boolean isImmutableType()              { return false; }
    @Override public TypeSerializer<TBox> duplicate()       { return INSTANCE; }
    @Override public TBox createInstance()                  { return null; }
    @Override public int getLength()                        { return -1; }
    @Override public boolean equals(Object o)               { return o instanceof TBoxSerializer; }
    @Override public int hashCode()                         { return TBoxSerializer.class.hashCode(); }

    @Override
    public TypeSerializerSnapshot<TBox> snapshotConfiguration() {
        return new TBoxSerializerSnapshot();
    }
}