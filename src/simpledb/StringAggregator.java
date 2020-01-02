package simpledb;

import simpledb.util.AggregateUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupField;

    private Type groupFieldType;

    private int aggregateField;

    private Type aggregateFieldType;

    private Op op;

    private Map<Field, List<Tuple>> group;

    private boolean noGrouping;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateFieldType = new IntField(0).getType();
        this.op = what;
        this.group = new HashMap<>();
        this.noGrouping = gbfield == NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("all")
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field;
        if (noGrouping) {
            field = IntegerAggregator.DEFAULT_FIELD;
        } else {
            field = tup.getField(groupField);
            tup.getField(groupField);
        }
        if (!group.containsKey(field)) {
            group.put(field, new ArrayList<>());
        }
        List<Tuple> list = group.get(field);
        list.add(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @SuppressWarnings("all")
    public OpIterator iterator() {
        Type[] types;
        if (noGrouping) {
            types = new Type[1];
            types[0] = aggregateFieldType;
        } else {
            types = new Type[2];
            types[0] = groupFieldType;
            types[1] = aggregateFieldType;
        }
        //聚合操作结果的表结构
        TupleDesc tupleDesc = new TupleDesc(types);
        //完成聚合操作以后的所有元组
        List<Tuple> aggregateTuple = new ArrayList<>();

        if (this.op != Op.COUNT) throw new IllegalArgumentException();

        for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
            int count = entry.getValue().size();
            aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, count, noGrouping));
        }
        return new TupleIterator(tupleDesc, aggregateTuple);
    }
}
