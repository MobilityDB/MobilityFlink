package sql.types.stbox;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.boxes.STBox;

import java.io.IOException;

public class STBoxSerializer extends TypeSerializer<STBox> {

    public static final STBoxSerializer INSTANCE = new STBoxSerializer();

    @Override
    public void serialize(STBox value, DataOutputView target) throws IOException {
        target.writeUTF(value.toString(6)); // Stbox_out
    }

    @Override
    public STBox deserialize(DataInputView source) throws IOException {
        return new STBox(source.readUTF()); // Stbox_in
    }

    @Override
    public STBox deserialize(STBox reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public STBox copy(STBox from) {
        return new STBox(from.toString(6)); // round-trip, never copy pointer
    }

    @Override
    public STBox copy(STBox from, STBox reuse) { return copy(from); }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
    }

    @Override public boolean isImmutableType()              { return false; }
    @Override public TypeSerializer<STBox> duplicate()       { return INSTANCE; }
    @Override public STBox createInstance()                  { return null; }
    @Override public int getLength()                        { return -1; }
    @Override public boolean equals(Object o)               { return o instanceof sql.types.stbox.STBoxSerializer; }
    @Override public int hashCode()                         { return sql.types.stbox.STBoxSerializer.class.hashCode(); }

    @Override
    public TypeSerializerSnapshot<STBox> snapshotConfiguration() {
        return new STBoxSerializerSnapshot();
    }
}