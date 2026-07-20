package sql.types.floatspan;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import sql.types.tbox.TBoxSerializer;
import types.collections.number.FloatSpan;

import java.io.IOException;

public class FloatSpanSerializerSnapshot implements TypeSerializerSnapshot<FloatSpan> {

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(FloatSpan.class.getName());

    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        in.readUTF();
    }

    @Override
    public TypeSerializer<FloatSpan> restoreSerializer() {
        return FloatSpanSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<FloatSpan> resolveSchemaCompatibility(TypeSerializerSnapshot<FloatSpan> oldSerializerSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
}