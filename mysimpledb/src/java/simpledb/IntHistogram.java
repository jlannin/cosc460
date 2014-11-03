package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int max;
	private int min;
	private int[] histogram;
	private int perbucket;
	private int lastbucket;

	/**
	 * Create a new IntHistogram.
	 * <p/>
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * <p/>
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * <p/>
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed.  For example, you shouldn't
	 * simply store every value that you see in a sorted list.
	 *
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		this.max = max;
		this.min = min;
		int distinctvalues = (max - min) + 1;

		if (distinctvalues < buckets)
		{
			perbucket = 1;
			lastbucket = 1;
			histogram = new int[distinctvalues];
		}
		else
		{
			perbucket = distinctvalues / buckets;
			lastbucket = (distinctvalues - (buckets*perbucket)) + perbucket;
			histogram = new int[buckets];
		}  
		for (int i = 0; i < histogram.length; i++)
		{
			histogram[i] = 0;
		}

	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		if (v > max || v < min)
		{
			throw new RuntimeException();
		}
		int index = findbucket(v);
		histogram[index]++;
	}

	private int findbucket(int v)
	{
		int bucket = (v - min)/perbucket;
		if (bucket >= histogram.length)
		{
			bucket = histogram.length-1;
		}
		return bucket;

	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * <p/>
	 * For example, if "op" is "GREATER_THAN" and "v" is 5,
	 * return your estimate of the fraction of elements that are greater than 5.
	 *
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {

		double equals = equals(v);
		double greaterthan = greaterthan(v);
		double lessthan = 1 - equals - greaterthan;

		if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE)
		{
			return equals;
		}
		if (op == Predicate.Op.GREATER_THAN)
		{
			return greaterthan;
		}
		if (op == Predicate.Op.LESS_THAN)
		{
			return lessthan;
		}  	
		if (op == Predicate.Op.LESS_THAN_OR_EQ)
		{
			return equals + lessthan;
		}
		if (op == Predicate.Op.GREATER_THAN_OR_EQ)
		{
			return equals + greaterthan;
		}
		if (op == Predicate.Op.NOT_EQUALS)
		{
			return 1 - equals;
		}
		throw new IllegalStateException("impossible to reach here");
	}

	private double equals(int v)
	{
		if (v > max || v < min)
		{
			return 0;
		}
		int b = findbucket(v);
		double expected = 0;
		if (b == histogram.length - 1)
		{
			expected = histogram[b] / (double) (((max - min) + 1) - ((histogram.length - 1)*perbucket));
		}
		else
		{
			expected = histogram[b]/(double)perbucket;
		}
		return expected / totalCount();
	}

	private double greaterthan(int v)
	{
		if (v > max)
		{
			return 0;
		}
		if (v < min)
		{
			return 1;
		}
		int b = findbucket(v);
		double frac = 0;

		if (b == histogram.length - 1)
		{
			int nums = (max - v);
			double one = (histogram[b]/(double)lastbucket)/totalCount();
			return (one * nums);
		}
		else
		{
			int nums = (min - 1) + ((b+1) * (perbucket)) - v;
			double one = (histogram[b]/(double)perbucket)/totalCount();
			frac = one * nums;
			b++;
		}
		while (b < histogram.length)
		{
			frac += bucketFraction(b);
			b++;
		}
		return frac;

	}

	private double bucketFraction(int bucket)
	{
		double frac = (histogram[bucket] / ((double)totalCount()));
		return frac;
	}

	private int totalCount()
	{
		int total = 0;
		for (int i = 0; i < histogram.length; i++)
		{
			total += histogram[i];
		}
		return total;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		String str = "";
		for(int x = 0; x < histogram.length; x++)
		{
			int low = min + (x*perbucket);
			int high;
			if(x == histogram.length-1)
			{
				high = max;
			}
			else
			{
				high = min + ((x+1)*perbucket) - 1;
			}
			str += Integer.toString(low) + "-" + Integer.toString(high) + ": ";
			str += Integer.toString(histogram[x]) + "\n";
		}
		return str;
	}
}
