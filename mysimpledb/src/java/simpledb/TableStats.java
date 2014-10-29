package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TupleDesc.TDItem;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

	class HelpNode
	{
		private Type fieldType;
		private int min;
		private int max;
		private int distinctValues;
		private HashSet<Integer> distinctInts;
		private HashSet<String> distinctStrings;

		public HelpNode(Type type, int num)
		{
			fieldType = type;
			if(fieldType == Type.INT_TYPE)
			{
				this.min = num;
				this.max = num;
				distinctInts = new HashSet<Integer>();
			}
			else
			{
				distinctStrings = new HashSet<String>();
			}
			distinctValues = 0;
		}

		public void checkDistinct(int x)
		{
			if(!distinctInts.contains(x))
			{
				distinctValues++;
				distinctInts.add(x);
			}
			checkMinMax(x);
		}

		public void checkDistinct(String s)
		{
			if(!distinctStrings.contains(s))
			{
				distinctValues++;
				distinctStrings.add(s);
			}
		}

		public int getMin()
		{
			if(fieldType == Type.INT_TYPE)
			{
				return min;
			}
			else
			{
				throw new RuntimeException("No Min for STRING_TYPE");
			}
		}

		public int getMax()
		{	
			if (fieldType == Type.INT_TYPE)
			{
				return max;
			}
			else
			{
				throw new RuntimeException("No Max for STRING_TYPE");
			}
		}

		public int getDistinct()
		{
			return distinctValues;
		}
		
		private void checkMinMax(int x)
		{
			if(x > max)
			{
				max = x;
			}
			else if (x < min)
			{
				min = x;
			}
		}

	}

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
	int costPerPage = IOCOSTPERPAGE;
	final int numpages;
	int numtuples;
	TupleDesc td;

	HashMap<Integer, Integer> distinctVals;
	HashMap<String, IntHistogram> intStats;
	HashMap<String, StringHistogram> stringStats;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 *
	 * @param tableid       The table over which to compute statistics
	 * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
	 *                      sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		costPerPage = ioCostPerPage;
		intStats = new HashMap<String, IntHistogram>();
		stringStats = new HashMap<String, StringHistogram>();
		distinctVals = new HashMap<Integer, Integer>();
		HeapFile db = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
		numpages = db.numPages();
		td = db.getTupleDesc();
		int numfields = td.numFields();
		int count = 0;
		DbFileIterator iter = db.iterator(new TransactionId());
		HashMap<String,HelpNode> calculations = new HashMap<String, HelpNode>();
		try {
			iter.open();
			if(iter.hasNext())
			{
				Tuple tp = iter.next();
				for (int i = 0; i < numfields; i++)
				{
					Type type = td.getFieldType(i);
					String name = td.getFieldName(i);
					if(type == Type.INT_TYPE)
					{
						int value = ((IntField)tp.getField(i)).getValue();
						calculations.put(name, new HelpNode(type, value));	
						calculations.get(name).checkDistinct(value);
					}
					else
					{
						String value = ((StringField)tp.getField(i)).getValue();
						calculations.put(name, new HelpNode(type, 0));
						calculations.get(name).checkDistinct(value);
					}
				}
				count++;
			}
			while(iter.hasNext())
			{
				Tuple tp = iter.next();
				for (int i = 0; i < numfields; i++)
				{
					Type type = td.getFieldType(i);
					String name = td.getFieldName(i);
					if(type == Type.INT_TYPE)
					{
						int value = ((IntField)tp.getField(i)).getValue();
						HelpNode node = calculations.get(name);
						node.checkDistinct(value);
					}
					else
					{
						String value = ((StringField)tp.getField(i)).getValue();
						HelpNode node = calculations.get(name);
						node.checkDistinct(value);
					}
				}
				count++;
			}
			iter.close();
			numtuples = count;
		}
		catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < numfields; i++)
		{
			Type type = td.getFieldType(i);
			String name = td.getFieldName(i);
			if(type == Type.INT_TYPE)
			{
				HelpNode help = calculations.get(name);
				intStats.put(name, new IntHistogram(NUM_HIST_BINS, help.getMin(), help.getMax()));
				distinctVals.put(Integer.valueOf(i), help.getDistinct());
			}
			else
			{
				HelpNode help = calculations.get(name);
				stringStats.put(name, new StringHistogram(NUM_HIST_BINS));
				distinctVals.put(Integer.valueOf(i), help.getDistinct());
			}
		}
		DbFileIterator iter2 = db.iterator(new TransactionId());
		try {
			iter2.open();
			while(iter2.hasNext())
			{
				Tuple tp = iter2.next();
				for (int i = 0; i < numfields; i++)
				{
					Type type = td.getFieldType(i);
					String name = td.getFieldName(i);
					if(type == Type.INT_TYPE)
					{
						int value = ((IntField)tp.getField(i)).getValue();
						intStats.get(name).addValue(value);
					}
					else
					{
						String value = ((StringField)tp.getField(i)).getValue();
						stringStats.get(name).addValue(value);
					}
				}
			}
			iter2.close();
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * <p/>
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 *
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {

		return numpages * costPerPage;
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
		if (selectivityFactor > 0 && selectivityFactor < (1/numtuples))
		{
			return 1;
		}
		return (int) Math.ceil(numtuples*selectivityFactor);
	}

	/**
	 * This method returns the number of distinct values for a given field.
	 * If the field is a primary key of the table, then the number of distinct
	 * values is equal to the number of tuples.  If the field is not a primary key
	 * then this must be explicitly calculated.  Note: these calculations should
	 * be done once in the constructor and not each time this method is called. In
	 * addition, it should only require space linear in the number of distinct values
	 * which may be much less than the number of values.
	 *
	 * @param field the index of the field
	 * @return The number of distinct values of the field.
	 */
	public int numDistinctValues(int field) {
		return distinctVals.get(field);

	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 *
	 * @param field    The field over which the predicate ranges
	 * @param op       The logical operation in the predicate
	 * @param constant The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 * predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		if(td.getFieldType(field) == Type.INT_TYPE)
		{
			if(constant.getType() != Type.INT_TYPE)
			{
				throw new RuntimeException("Constant type does not match predicate field type");
			}
			return intStats.get(td.getFieldName(field)).estimateSelectivity(op, ((IntField)constant).getValue());
		}
		else
		{
			if(constant.getType() != Type.STRING_TYPE)
			{
				throw new RuntimeException("Constant type does not match predicate field type");
			}
			return stringStats.get(td.getFieldName(field)).estimateSelectivity(op, ((StringField)constant).getValue());	
		}
	}

}
