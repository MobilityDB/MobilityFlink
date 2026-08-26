package sql.types.floatspan;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import types.collections.number.FloatSpan;

import java.io.IOException;

public class FloatSpanSerializer extends TypeSerializer<FloatSpan> {

    public static final FloatSpanSerializer INSTANCE = new FloatSpanSerializer();

    // --- Serialization boundary: the ONLY place FloatSpan crosses the JVM boundary ---
    // FloatSpan(String) calls floatspan_in via JMEOS
    // toString(6)       calls floatspan_out via JMEOS
    // No Java object serialization, no native pointer copying.

    @Override
    public void serialize(FloatSpan value, DataOutputView target) throws IOException {
        target.writeUTF(value.toString(6)); // floatspan_out → e.g. "[2.5, 5.21]"
    }

    @Override
    public FloatSpan deserialize(DataInputView source) throws IOException {
        return new FloatSpan(source.readUTF()); // floatspan_in
    }

    @Override
    public FloatSpan deserialize(FloatSpan reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public FloatSpan copy(FloatSpan from) {
        return new FloatSpan(from.toString(6)); // round-trip, never copy pointer
    }

    @Override
    public FloatSpan copy(FloatSpan from, FloatSpan reuse) { return copy(from); }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
    }

    @Override public boolean isImmutableType()  { return false; }
    @Override public TypeSerializer<FloatSpan> duplicate() { return INSTANCE; }
    @Override public FloatSpan createInstance() { return null; }
    @Override public int getLength()            { return -1; }

    @Override public boolean equals(Object o)   { return o instanceof FloatSpanSerializer; }
    @Override public int hashCode()             { return FloatSpanSerializer.class.hashCode(); }

    @Override
    public TypeSerializerSnapshot<FloatSpan> snapshotConfiguration() {
        return new FloatSpanSerializerSnapshot();
    }
}