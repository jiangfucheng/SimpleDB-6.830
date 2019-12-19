package simpledb.util;

import simpledb.Field;
import simpledb.IntField;
import simpledb.Tuple;
import simpledb.TupleDesc;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * Date: 2019/12/10
 * Time: 15:24
 *
 * @author jiangfucheng
 */
public class AggregateUtil {
    public static Tuple fillTuple(TupleDesc tupleDesc, Map.Entry<Field, List<Tuple>> entry, int val, boolean noGrouping) {
        Tuple tuple = new Tuple(tupleDesc);
        if (noGrouping) {
            tuple.setField(0, new IntField(val));
        } else {
            tuple.setField(0, entry.getKey());
            tuple.setField(1, new IntField(val));
        }
        return tuple;
    }

}
