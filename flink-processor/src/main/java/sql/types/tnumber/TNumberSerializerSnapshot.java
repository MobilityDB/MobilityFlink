package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.basic.tnumber.TNumber;

import java.io.IOException;

public abstract class TNumberSerializerSnapshot<T extends TNumber>
        implements TypeSerializerSnapshot<T> {

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(serializerClass().getName());
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader)
            throws IOException {
        in.readUTF(); // consume the class name written above
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    // Subclass returns its concrete serializer class for snapshot identification
    protected abstract Class<? extends TNumberSerializer<T>> serializerClass();
}