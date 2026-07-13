package sql.types;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.collections.number.FloatSpan;

public class FloatSpanTypeInfo extends TypeInformation<FloatSpan> {

    public static final FloatSpanTypeInfo INSTANCE = new FloatSpanTypeInfo();

    @Override public boolean isBasicType()       { return false; }
    @Override public boolean isTupleType()       { return false; }
    @Override public int getArity()              { return 1; }
    @Override public int getTotalFields()        { return 1; }
    @Override public Class<FloatSpan> getTypeClass() { return FloatSpan.class; }
    @Override public boolean isKeyType()         { return false; }

    @Override
    public TypeSerializer<FloatSpan> createSerializer(SerializerConfig config) {
        return FloatSpanSerializer.INSTANCE;
    }

    @Override public String toString()              { return "FloatSpan"; }
    @Override public boolean equals(Object o)       { return o instanceof FloatSpanTypeInfo; }
    @Override public int hashCode()                 { return FloatSpanTypeInfo.class.hashCode(); }
    @Override public boolean canEqual(Object obj)   { return obj instanceof FloatSpanTypeInfo; }
}