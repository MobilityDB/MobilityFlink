package sql.types.tbox;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import types.boxes.TBox;

public class TBoxSerializerSnapshot
        extends SimpleTypeSerializerSnapshot<TBox> {

    public TBoxSerializerSnapshot() {
        super(() -> TBoxSerializer.INSTANCE);
    }
}