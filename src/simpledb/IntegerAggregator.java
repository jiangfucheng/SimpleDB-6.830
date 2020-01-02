package simpledb;

import simpledb.util.AggregateUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;

    private Type groupByFieldType;

    private int aggregateField;

    private Type aggregateFieldType;

    private Op op;

    private Map<Field, List<Tuple>> group;

    private boolean noGrouping;

    public static final Field DEFAULT_FIELD = new IntField(0);

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateFieldType = new IntField(0).getType();
        this.op = what;
        this.group = new HashMap<>();
        this.noGrouping = gbfield == NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field;
        if (noGrouping) {
            field = DEFAULT_FIELD;
        } else {
            field = tup.getField(groupByField);
            tup.getField(groupByField);
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        Type[] types;
        if (noGrouping) {
            types = new Type[1];
            types[0] = aggregateFieldType;
        } else {
            types = new Type[2];
            types[0] = groupByFieldType;
            types[1] = aggregateFieldType;
        }
        //聚合操作结果的表结构
        TupleDesc tupleDesc = new TupleDesc(types);
        //完成聚合操作以后的所有元组
        List<Tuple> aggregateTuple = new ArrayList<>();
        switch (op) {
            case AVG: {
                for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                    int sum = 0;
                    for (Tuple temp : entry.getValue()) {
                        sum += Integer.parseInt(temp.getField(aggregateField).toString());
                    }
                    int avg = sum / entry.getValue().size();
                    aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, avg, noGrouping));
                }
                break;
            }
            case MAX: {
                for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                    int max = Integer.MIN_VALUE;
                    for (Tuple temp : entry.getValue()) {
                        max = Math.max(max, Integer.parseInt(temp.getField(aggregateField).toString()));
                    }
                    aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, max, noGrouping));
                }
                break;
            }
            case MIN: {
                for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                    int min = Integer.MAX_VALUE;
                    for (Tuple temp : entry.getValue()) {
                        min = Math.min(min, Integer.parseInt(temp.getField(aggregateField).toString()));
                    }
                    aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, min, noGrouping));
                }
                break;
            }
            case SUM: {
                for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                    int sum = 0;
                    for (Tuple temp : entry.getValue()) {
                        sum += Integer.parseInt(temp.getField(aggregateField).toString());
                    }
                    aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, sum, noGrouping));
                }
                break;
            }
            case COUNT: {
                for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                    int count = entry.getValue().size();
                    aggregateTuple.add(AggregateUtil.fillTuple(tupleDesc, entry, count, noGrouping));
                }
            }
        }
        return new TupleIterator(tupleDesc, aggregateTuple);
    }


}
