package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator iter;
	private int tableid;
	private TupleDesc returntd;
	private boolean inserted = false;
	
	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableid The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we are to
	 *                     insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableid)
			throws DbException {
		tid = t;
		iter = child;
		this.tableid = tableid;
		String[] field = new String [] {"count"};
		Type [] inttype = new Type[] {Type.INT_TYPE};
		returntd = new TupleDesc(inttype, field);
		TupleDesc childtd = child.getTupleDesc();
		TupleDesc tabletd = Database.getCatalog().getTupleDesc(tableid);
		
		if (!tabletd.equals(childtd))
		{
			throw new DbException("Child TupleDesc differs from the TupleDesc of the table!");
		}
	}

	public TupleDesc getTupleDesc() {
		return returntd;
	}

	public void open() throws DbException, TransactionAbortedException {
		iter.open();
		super.open();
	}

	public void close() {
		iter.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		iter.rewind();
		inserted = false;
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 * null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if (inserted)
		{
			return null;
		}
		
		BufferPool bp = Database.getBufferPool();
		int count = 0;
		while(iter.hasNext())
		{
			try {
				bp.insertTuple(tid,  tableid, iter.next());
				count++;
			}
		    catch (IOException e) {
				throw new DbException("IO Exception during insert occured!");
			}
		}
		inserted = true;
		Tuple retuple = new Tuple(returntd);
		retuple.setField(0, new IntField(count));
		return retuple;
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
