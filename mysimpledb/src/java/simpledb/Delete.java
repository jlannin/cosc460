package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
	private DbIterator iter;
	private TupleDesc returntd;
	private boolean deleted = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        tid = t;
        iter = child;
        String[] field = new String [] {"count"};
		Type [] inttype = new Type[] {Type.INT_TYPE};
		returntd = new TupleDesc(inttype, field);
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
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if (deleted)
		{
			return null;
		}
    	BufferPool bp = Database.getBufferPool();
		int count = 0;
		while(iter.hasNext())
		{
			try {
				bp.deleteTuple(tid, iter.next());
				count++;
			}
		    catch (IOException e) {
				throw new DbException("IO Exception during insert occured!");
			}
		}
		Tuple retuple = new Tuple(returntd);
		retuple.setField(0, new IntField(count));
		deleted = true;
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
