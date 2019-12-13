package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

	static final int IOCOSTPERPAGE = 1000;

	public static TableStats getTableStats(String tablename) {
		return statsMap.get(tablename);
	}

	public static void setTableStats(String tablename, TableStats stats) {
		statsMap.put(tablename, stats);
	}

	public static void setStatsMap(HashMap<String, TableStats> s) {
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

	}

	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}

	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);
		}
		System.out.println("Done.");
	}

	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;

	private DbFile table;

	private int ioCostPerPage;

	private int ntups;

	private int nPages;

	private int[] fieldMaxVal;

	private int[] fieldMinVal;

	List<List<Object>> fields;

	private Map<Integer, Object> histogram;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 *
	 * @param tableid       The table over which to compute statistics
	 * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
	 *                      sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		this.ioCostPerPage = ioCostPerPage;
		table = Database.getCatalog().getDatabaseFile(tableid);
		initFields();

		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		// some code goes here
	}

	private void initFields() {
		TransactionId transactionId = new TransactionId();
		DbFileIterator iterator = table.iterator(transactionId);
		TupleDesc tupleDesc = table.getTupleDesc();
		this.fieldMaxVal = new int[tupleDesc.numFields()];
		this.fieldMinVal = new int[tupleDesc.numFields()];
		Arrays.fill(fieldMaxVal, Integer.MIN_VALUE);
		Arrays.fill(fieldMinVal, Integer.MAX_VALUE);
		fields = new ArrayList<>();
		for (int i = 0; i < tupleDesc.numFields(); i++)
			fields.add(new ArrayList<>());
		try {
			iterator.open();
			Set<PageId> set = new HashSet<>();
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				PageId pageId = tuple.getRecordId().getPageId();
				if (!set.contains(pageId)) {
					nPages++;
					set.add(pageId);
				}
				ntups++;
				//统计所有属性的最大值和最小值
				for (int i = 0; i < tupleDesc.numFields(); i++) {
					fields.get(i).add(tuple.getField(i));
					if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) continue;
					int fieldVal = Integer.parseInt(tuple.getField(i).toString());
					fieldMinVal[i] = Math.min(fieldMinVal[i], fieldVal);
					fieldMaxVal[i] = Math.max(fieldMaxVal[i], fieldVal);
				}
			}
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * <p>
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 *
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		return nPages * ioCostPerPage;
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 *
	 * @param selectivityFactor The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 * selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		return (int) (ntups * selectivityFactor);
	}

	/**
	 * The average selectivity of the field under op.
	 *
	 * @param field the index of the field
	 * @param op    the operator in the predicate
	 *              The semantic of the method is that, given the table, and then given a
	 *              tuple, of which we do not know the value of the field, return the
	 *              expected selectivity. You may estimate this value from the histograms.
	 */
	public double avgSelectivity(int field, Predicate.Op op) {


		return 1.0;
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 *
	 * @param i        The field over which the predicate ranges
	 * @param op       The logical operation in the predicate
	 * @param constant The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 * predicate
	 */
	public double estimateSelectivity(int i, Predicate.Op op, Field constant) {
		Type type = table.getTupleDesc().getFieldType(i);
		if (type == Type.INT_TYPE) {
			IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, fieldMinVal[i], fieldMaxVal[i]);
			for (Object fieldVal : fields.get(i)) {
				intHistogram.addValue(((IntField) fieldVal).getValue());
			}
			return intHistogram.estimateSelectivity(op, Integer.parseInt(constant.toString()));
		} else {
			StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
			for (Object fieldVal : fields.get(i)) {
				stringHistogram.addValue(((StringField) fieldVal).getValue());
			}
			return stringHistogram.estimateSelectivity(op, constant.toString());
		}
	}

	/**
	 * return the total number of tuples in this table
	 */
	public int totalTuples() {
		return ntups;
	}

}
