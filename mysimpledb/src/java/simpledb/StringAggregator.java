package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 * 
 * See the lab3 write up for details on how the Aggregate is implemented
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int groupindex;
	private Type gtype;
	private int aggfield;
	private Aggregator.Op operator;
	private boolean grouping = true;
	private TupleDesc td;
	private ConcurrentHashMap<Field, Tuple> groupbyagg;
	private IntField nogroupkey = new IntField(0);

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
		groupindex = gbfield;
		if (groupindex == Aggregator.NO_GROUPING)
		{
			grouping = false;
		}
		gtype = gbfieldtype;
		aggfield = afield;
		operator = what;
		groupbyagg = new ConcurrentHashMap<Field, Tuple>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		if (!grouping)
		{
			if (!groupbyagg.containsKey(nogroupkey))
			{	
				groupbyagg.put(nogroupkey, createNoGroupTuple(tup));
			}
			Tuple update = groupbyagg.get(nogroupkey);
			update = aggregate(update, tup, 0);
			groupbyagg.put(nogroupkey, update);
		}
		else // we are grouping
		{
			Field groupfield = tup.getField(groupindex);
			if (!groupbyagg.containsKey(groupfield))
			{
				groupbyagg.put(groupfield, createGroupTuple(groupfield, tup));
			}
			Tuple update = groupbyagg.get(groupfield);
			update = aggregate(update, tup, 1);
			groupbyagg.put(groupfield, update);
		}    
	}

	private Tuple aggregate(Tuple oldtup, Tuple newtup, int loc)
	{
		int value = ((IntField) oldtup.getField(loc)).getValue();
		value++;
		if (operator == Aggregator.Op.COUNT)
		{
			oldtup.setField(loc, new IntField(value));
		}
		else
		{
			throw new RuntimeException("Bad Operator!");
		}
		return oldtup;
	}
	
	//create a tuple in case of no grouping
	private Tuple createNoGroupTuple(Tuple tup)
	{
		Type[] types = new Type[] {Type.INT_TYPE};
		String fieldname = operator.toString() + (tup.getTupleDesc()).getFieldName(aggfield);
		String[] field = new String[] {fieldname};
		TupleDesc td = new TupleDesc(types, field);
		Tuple newtup = new Tuple(td);
		newtup.setField(0, new IntField(0));
		return newtup;
	}

	//create a tuple with grouping
	private Tuple createGroupTuple(Field value, Tuple tup)
	{
		Type[] types = new Type[] {gtype, Type.INT_TYPE};
		String fieldname = operator.toString() + (tup.getTupleDesc()).getFieldName(aggfield);
		String[] field = new String [] {(tup.getTupleDesc()).getFieldName(groupindex), fieldname};
		TupleDesc td = new TupleDesc(types, field);
		Tuple newtup = new Tuple(td);
		newtup.setField(0, value);
		newtup.setField(1, new IntField(0));
		return newtup;
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 * aggregateVal) if using group, or a single (aggregateVal) if no
	 * grouping. The aggregateVal is determined by the type of
	 * aggregate specified in the constructor.
	 */
	public DbIterator iterator()
	{
			return new groupiterator();
	}

	class groupiterator implements DbIterator {
		private boolean open = false;
		private Iterator<Tuple> iter;
		private TupleDesc td = null;

		public groupiterator()
		{
			iter = (groupbyagg.values()).iterator();
			if (iter.hasNext())
			{
				td = iter.next().getTupleDesc();
			}
			iter = (groupbyagg.values()).iterator();
		}

		public void open() throws DbException, TransactionAbortedException
		{
			open = true;
		}

		/**
		 * Returns true if the iterator has more tuples.
		 *
		 * @return true f the iterator has more tuples.
		 * @throws IllegalStateException If the iterator has not been opened
		 */
		public boolean hasNext() throws DbException, TransactionAbortedException
		{
			if (!open) 
			{
				throw new NoSuchElementException("Iterator Not Open!");
			}
			return iter.hasNext();
		}

		/**
		 * Returns the next tuple from the operator (typically implementing by reading
		 * from a child operator or an access method).
		 *
		 * @return the next tuple in the iteration.
		 * @throws NoSuchElementException if there are no more tuples.
		 * @throws IllegalStateException  If the iterator has not been opened
		 */
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
		{
			if (!open) 
			{
				throw new NoSuchElementException("Iterator Not Open!");
			}
			if (!iter.hasNext())
			{
				throw new NoSuchElementException();
			}
			else
			{
				return iter.next();
			}
		}

		/**
		 * Resets the iterator to the start.
		 *
		 * @throws DbException           when rewind is unsupported.
		 * @throws IllegalStateException If the iterator has not been opened
		 */
		public void rewind() throws DbException, TransactionAbortedException
		{
			if (!open) 
			{
				throw new NoSuchElementException("Iterator Not Open!");
			}
			iter = (groupbyagg.values()).iterator();
		}

		/**
		 * Returns the TupleDesc associated with this DbIterator.
		 *
		 * @return the TupleDesc associated with this DbIterator.
		 */
		public TupleDesc getTupleDesc()
		{
			return td;
		}

		/**
		 * Closes the iterator. When the iterator is closed, calling next(),
		 * hasNext(), or rewind() should fail by throwing IllegalStateException.
		 */
		public void close()
		{
			open = false;
		}
	}
}
