package sql.udf.tnumber;

import functions.functions;
import jnr.ffi.Pointer;
import org.apache.flink.table.functions.AggregateFunction;
import types.basic.tfloat.TFloatSeq;

import java.util.ArrayList;
import java.util.List;

public class TFloatTAvg extends AggregateFunction<String, TFloatTAvg.Accumulator> {

    public static class Accumulator {
        public List<String> wkts = new ArrayList<>();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, String wkt) {
        if (wkt == null) return;
        acc.wkts.add(wkt);
    }

    @Override
    public String getValue(Accumulator acc) {
        if (acc.wkts.isEmpty()) return null;
        Pointer state = null;
        for (String wkt : acc.wkts) {
            state = functions.tnumber_tavg_transfn(state, new TFloatSeq(wkt).getNumberInner());
        }
        Pointer resultPtr = functions.tnumber_tavg_finalfn(state);
        if (resultPtr == null) return null;
        return new TFloatSeq(resultPtr).as_wkt(6);
    }
}