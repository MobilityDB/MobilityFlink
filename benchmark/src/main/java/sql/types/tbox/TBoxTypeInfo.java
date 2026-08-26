package sql.types.tbox;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.boxes.TBox;

public class TBoxTypeInfo extends TypeInformation<TBox> {

    public static final TBoxTypeInfo INSTANCE = new TBoxTypeInfo();

    @Override public boolean isBasicType()              { return false; }
    @Override public boolean isTupleType()              { return false; }
    @Override public int getArity()                     { return 1; }
    @Override public int getTotalFields()               { return 1; }
    @Override public Class<TBox> getTypeClass()         { return TBox.class; }
    @Override public boolean isKeyType()                { return false; }

    @Override
    public TypeSerializer<TBox> createSerializer(SerializerConfig config) {
        return TBoxSerializer.INSTANCE;
    }

    @Override public String toString()                  { return "TBox"; }
    @Override public boolean equals(Object o)           { return o instanceof TBoxTypeInfo; }
    @Override public int hashCode()                     { return TBoxTypeInfo.class.hashCode(); }
    @Override public boolean canEqual(Object obj)       { return obj instanceof TBoxTypeInfo; }
}