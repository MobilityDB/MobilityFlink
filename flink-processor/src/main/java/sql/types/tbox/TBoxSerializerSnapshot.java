package sql.types.tbox;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.boxes.TBox;

import java.io.IOException;

public class TBoxSerializerSnapshot implements TypeSerializerSnapshot<TBox> {

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        // Writes the class name so Flink can identify it on restore
        out.writeUTF(TBoxSerializer.class.getName());
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
            throws IOException {
        in.readUTF(); // Consumes the class name written above
    }

    @Override
    public TypeSerializer<TBox> restoreSerializer() {
        return TBoxSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<TBox> resolveSchemaCompatibility(
            TypeSerializerSnapshot<TBox> oldSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
}