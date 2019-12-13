package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private List<List<Integer>> histogram;

	private int buckets;

	private int min;

	private int max;

	private int bucketWidth;

	private int ntups;

	private int counter;

	private double totalSelectivity;

	/**
	 * Create a new IntHistogram.
	 * <p>
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * <p>
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * <p>
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed.  For example, you shouldn't
	 * simply store every value that you see in a sorted list.
	 *
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		this.buckets = buckets;
		if(this.buckets > max - min + 1)
			this.buckets = max - min + 1;
		this.min = min;
		this.max = max;
		this.ntups = 0;
		this.bucketWidth = (max - min + 1) / buckets;
		if (this.bucketWidth <= 0) bucketWidth = 1;
		this.histogram = new ArrayList<>();
		for (int i = 0; i < this.buckets; i++)
			histogram.add(new ArrayList<>());
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * min = 100,max = 110 buckets = 10 1000 / 100
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		int idx = getPosition(v);
		histogram.get(idx).add(v);
		ntups++;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * <p>
	 * For example, if "op" is "GREATER_THAN" and "v" is 5,
	 * return your estimate of the fraction of elements that are greater than 5.
	 *
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		double res = 0;
		switch (op) {
			case EQUALS: {
				res = estimateEqualSelectivity(v);
				break;
			}
			case LESS_THAN_OR_EQ: {
				res = estimateLessSelectivity(true, v);
				break;
			}
			case LESS_THAN: {
				res = estimateLessSelectivity(false, v);
				break;
			}
			case GREATER_THAN: {
				res = estimateGreaterSelectivity(false, v);
				break;
			}
			case GREATER_THAN_OR_EQ: {
				res = estimateGreaterSelectivity(true, v);
				break;
			}
			case NOT_EQUALS: {
				res = 1 - estimateEqualSelectivity(v);
				break;
			}
		}
		totalSelectivity += res;
		counter++;
		return res;
	}

	private double estimateLessSelectivity(boolean hasEqual, int v) {
		if (v < min) return 0;
		if (v > max) return 1;
		double res = estimateRangeSelectivity(hasEqual, false, v);
		int idx = getPosition(v);
		for (int i = 0; i < idx; i++) {
			res += (double) histogram.get(i).size() / ntups;
		}
		return res;
	}

	private double estimateGreaterSelectivity(boolean hasEqual, int v) {
		if (v < min) return 1;
		if (v > max) return 0;
		int idx = getPosition(v);
		double res = estimateRangeSelectivity(hasEqual, true, v);
		for (int i = idx + 1; i < buckets; i++) {
			res += (double) histogram.get(i).size() / ntups;
		}
		return res;
	}

	private double estimateRangeSelectivity(boolean hasEqual, boolean greater, int v) {
		double res;
		int idx = getPosition(v);
		List<Integer> bucket = histogram.get(idx);
		double h = bucket.size();
		int right = this.min + idx * bucketWidth + (bucketWidth - 1);
		int left = this.min + idx * bucketWidth;
		double b_f = h / ntups;
		double range;
		if (greater) {
			range = (double) right - v;
		} else {
			range = (double) v - left;
		}
		if (hasEqual) range++;
		double b_part = range / bucketWidth;
		res = b_f * b_part;
		return res;
	}

	private double estimateEqualSelectivity(int v) {
		if (v < min || v > max) return 0;
		double res;
		int idx = getPosition(v);
		List<Integer> bucket = histogram.get(idx);
		double h = bucket.size();
		res = (h / bucketWidth) / ntups;
		return res;
	}

	private int getPosition(int v) {
		int idx = (v - min) / bucketWidth;
		if (idx >= buckets) idx = buckets - 1;
		if (idx <= 0) idx = 0;
		return idx;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * <p>
	 * This is not an indispensable method to implement the basic
	 * join optimization. It may be needed if you want to
	 * implement a more efficient optimization
	 */
	public double avgSelectivity() {
		return totalSelectivity / counter;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < buckets; i++) {
			builder.append("bucket: ")
					.append(i)
					.append(" h:")
					.append(histogram.get(i).size())
					.append("\n");
		}
		return builder.toString();
	}
}
