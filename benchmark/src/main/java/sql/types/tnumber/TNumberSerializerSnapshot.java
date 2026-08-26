package sql.types.tnumber;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.basic.tnumber.TNumber;

import java.util.function.Supplier;

public abstract class TNumberSerializerSnapshot<T extends TNumber>
        extends SimpleTypeSerializerSnapshot<T> {

    protected TNumberSerializerSnapshot(Supplier<? extends TypeSerializer<T>> serializerSupplier) {
        super(serializerSupplier);
    }
}