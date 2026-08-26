package sql.types.stbox;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import types.boxes.STBox;

public class STBoxSerializerSnapshot
        extends SimpleTypeSerializerSnapshot<STBox> {

    public STBoxSerializerSnapshot() {
        super(() -> STBoxSerializer.INSTANCE);
    }
}