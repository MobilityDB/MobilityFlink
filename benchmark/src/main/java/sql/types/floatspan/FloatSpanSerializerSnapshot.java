package sql.types.floatspan;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import types.collections.number.FloatSpan;

public class FloatSpanSerializerSnapshot
        extends SimpleTypeSerializerSnapshot<FloatSpan> {

    public FloatSpanSerializerSnapshot() {
        super(() -> FloatSpanSerializer.INSTANCE);
    }
}