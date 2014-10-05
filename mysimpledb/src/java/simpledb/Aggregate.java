package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 
 * See the lab3 write up for details on how the Aggregate is implemented
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;
	private DbIterator iter;
	private int aggfield;
	private int groupfield;
	private Aggregator.Op operator;
	private Aggregator agg;
	private boolean added = false;
	private DbIterator aggvalues = null;


	/**
	 * Constructor.
	 * <p/>
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 *
	 * @param child  The DbIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		iter = child;
		aggfield = afield;
		groupfield = gfield;
		operator = aop;
		if ((iter.getTupleDesc()).getFieldType(afield) == Type.INT_TYPE)
		{
			Type gtype = null;
			if (groupfield != Aggregator.NO_GROUPING)
			{
				gtype = (iter.getTupleDesc()).getFieldType(groupfield);
			}
			agg = new IntegerAggregator(groupfield, gtype, aggfield,operator);
		}
		else
		{
			Type gtype = null;
			if (groupfield != Aggregator.NO_GROUPING)
			{
				gtype = (iter.getTupleDesc()).getFieldType(groupfield);
			}
			agg = new StringAggregator(groupfield, gtype, aggfield,operator);
		}
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 * field index in the <b>INPUT</b> tuples. If not, return
	 * {@link simpledb.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		if (groupfield == Aggregator.NO_GROUPING)
		{
			return Aggregator.NO_GROUPING;
		}
		else
		{
			return groupfield;
		}
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 * of the groupby field in the <b>OUTPUT</b> tuples If not, return
	 * null;
	 */
	public String groupFieldName() {
		if (groupfield == Aggregator.NO_GROUPING)
		{
			return null;
		}
		else
		{
			return (iter.getTupleDesc()).getFieldName(groupfield);
		}
	}

	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		return aggfield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 * tuples
	 */
	public String aggregateFieldName() {
		return (iter.getTupleDesc()).getFieldName(aggfield);
	}

	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		return operator;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException,
	TransactionAbortedException {
		iter.open();
		super.open();
		if(!added)
		{
			while(iter.hasNext())
			{
				agg.mergeTupleIntoGroup(iter.next());
			}
			aggvalues = agg.iterator();
		}
		aggvalues.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if(aggvalues.hasNext())
		{
			return aggvalues.next();
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		iter.rewind();
		aggvalues.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * <p/>
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		String[] field = null;
		Type[] types = null;
		String fieldname = operator.toString() + (iter.getTupleDesc()).getFieldName(aggfield);
		if (groupfield == Aggregator.NO_GROUPING)
		{
			field = new String [] {fieldname};
			types = new Type[] {(iter.getTupleDesc()).getFieldType(aggfield)};
		}
		else
		{
			field = new String [] {(iter.getTupleDesc()).getFieldName(groupfield), fieldname};
			types = new Type[] {(iter.getTupleDesc()).getFieldType(groupfield), (iter.getTupleDesc()).getFieldType(aggfield)};
		}
		TupleDesc td = new TupleDesc(types, field);
		return td;
	}

	public void close() {
		iter.close();
		aggvalues.close();
		super.close();
	}

	@Override
	public DbIterator[] getChildren() {
		DbIterator[] iterarray = new DbIterator[] {iter};
		return iterarray;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		iter = children[0];
	}

}
