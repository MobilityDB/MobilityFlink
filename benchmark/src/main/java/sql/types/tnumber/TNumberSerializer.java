package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.basic.tnumber.TNumber;

import java.io.IOException;

public abstract class TNumberSerializer<T extends TNumber>
        extends TypeSerializer<T> {

    // Subclasses provide the concrete round-trip via JMEOS text functions
    protected abstract String serialize(T value);       // tnumber_out equivalent
    protected abstract T deserialize(String text);      // tnumber_in equivalent

    @Override
    public void serialize(T value, DataOutputView target) throws IOException {
        target.writeUTF(serialize(value));
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return deserialize(source.readUTF());
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public T copy(T from) {
        return deserialize(serialize(from)); // round-trip, never copy native pointer
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
    }

    @Override public boolean isImmutableType()  { return false; }
    @Override public int getLength()            { return -1; }
}