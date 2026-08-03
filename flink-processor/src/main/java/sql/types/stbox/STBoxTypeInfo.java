package sql.types.stbox;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.boxes.STBox;

public class STBoxTypeInfo extends TypeInformation<STBox> {

    public static final STBoxTypeInfo INSTANCE = new STBoxTypeInfo();

    @Override public boolean isBasicType()              { return false; }
    @Override public boolean isTupleType()              { return false; }
    @Override public int getArity()                     { return 1; }
    @Override public int getTotalFields()               { return 1; }
    @Override public Class<STBox> getTypeClass()         { return STBox.class; }
    @Override public boolean isKeyType()                { return false; }

    @Override
    public TypeSerializer<STBox> createSerializer(SerializerConfig config) {
        return STBoxSerializer.INSTANCE;
    }

    @Override public String toString()                  { return "STBox"; }
    @Override public boolean equals(Object o)           { return o instanceof STBoxTypeInfo; }
    @Override public int hashCode()                     { return STBoxTypeInfo.class.hashCode(); }
    @Override public boolean canEqual(Object obj)       { return obj instanceof STBoxTypeInfo; }
}