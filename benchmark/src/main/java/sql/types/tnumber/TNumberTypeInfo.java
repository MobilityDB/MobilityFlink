package sql.types.tnumber;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import types.basic.tnumber.TNumber;

public abstract class TNumberTypeInfo<T extends TNumber>
        extends TypeInformation<T> {

    @Override public boolean isBasicType()      { return false; }
    @Override public boolean isTupleType()      { return false; }
    @Override public int getArity()             { return 1; }
    @Override public int getTotalFields()       { return 1; }
    @Override public boolean isKeyType()        { return false; }

    @Override
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return serializer();
    }

    // Subclass returns its singleton serializer instance
    protected abstract TypeSerializer<T> serializer();

    @Override public String toString()              { return getTypeClass().getSimpleName(); }
    @Override public boolean canEqual(Object obj)   { return obj != null && obj.getClass() == getClass(); }
    @Override public boolean equals(Object o)       { return o != null && o.getClass() == getClass(); }
    @Override public int hashCode()                 { return getTypeClass().hashCode(); }
}