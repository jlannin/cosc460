package simpledb;

import java.io.IOException;

public class Lab3Main {

	/**
	 * @param args
	 */
	public static void main(String[] argv) 
			throws DbException, TransactionAbortedException, IOException {

		System.out.println("Loading schema from file:");
		// file named college.schema must be in mysimpledb directory
		Database.getCatalog().loadSchema("college.schema");

		// SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
		// algebra translation: select_{name="alice"}( Students )
		// query plan: a tree with the following structure
		// - a Filter operator is the root; filter keeps only those w/ name=Alice
		// - a SeqScan operator on Students at the child of root
		TransactionId tid = new TransactionId();
		SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
		SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
		JoinPredicate pred = new JoinPredicate((scanStudents.getTupleDesc()).fieldNameToIndex("students.sid"), Predicate.Op.EQUALS, (scanTakes.getTupleDesc()).fieldNameToIndex("takes.sid"));
		/*
		Looping the other way:
		JoinPredicate pred = new JoinPredicate((scanTakes.getTupleDesc()).fieldNameToIndex("takes.sid"), Predicate.Op.EQUALS, (scanStudents.getTupleDesc()).fieldNameToIndex("students.sid"));
		Join op = new Join(pred, scanTakes, scanStudents); 
		 */
		Join op = new Join(pred, scanStudents, scanTakes);
		op.open();
		// query execution: we open the iterator of the root and iterate through results
		System.out.println("Query results:");
		while (op.hasNext()) {
			Tuple tup = op.next();
			System.out.println("\t"+tup);
		}


		op.close();
		Database.getBufferPool().transactionComplete(tid);
	}

}
