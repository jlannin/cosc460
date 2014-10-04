package simpledb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

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
		SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"), "S");
		SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"), "T");
		SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("profs"), "P");
		
		//join S.sid = T.sid
		JoinPredicate pred = new JoinPredicate((scanStudents.getTupleDesc()).fieldNameToIndex("S.sid"), Predicate.Op.EQUALS, (scanTakes.getTupleDesc()).fieldNameToIndex("T.sid"));
		Join op = new Join(pred, scanStudents, scanTakes);
		
		//join result of previous join on T.cid = P.favoriteCourse
		JoinPredicate predcourses = new JoinPredicate((op.getTupleDesc()).fieldNameToIndex("T.cid"), Predicate.Op.EQUALS, (scanProfs.getTupleDesc()).fieldNameToIndex("P.favoriteCourse"));
		Join op1 = new Join(predcourses, op, scanProfs);
        
		//select those with P.name = "hay"
		StringField hay = new StringField("hay", Type.STRING_LEN);
		Predicate p = new Predicate((op1.getTupleDesc()).fieldNameToIndex("P.name"), Predicate.Op.EQUALS, hay);
		Filter filterdata = new Filter(p, op1);
		
		//project out the S.names
		ArrayList<Integer> proj = new ArrayList<Integer>();
		Type[] types = new Type[] {Type.STRING_TYPE};
		proj.add((op1.getTupleDesc()).fieldNameToIndex("S.name"));
		Project finaldata = new Project(proj, types, filterdata);
		
		//print results
		System.out.println(op.getTupleDesc());
		finaldata.open();
		// query execution: we open the iterator of the root and iterate through results
		System.out.println("Query results:");
		while (finaldata.hasNext()) {
			Tuple tup = finaldata.next();
			System.out.println("\t"+tup);
		}
		
		
	}

}
