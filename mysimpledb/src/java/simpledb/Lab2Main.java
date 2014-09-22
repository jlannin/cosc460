package simpledb;

import java.io.File;
import java.io.IOException;

public class Lab2Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{ "field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        
        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        
        
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                if(((IntField) tup.getField(1)).getValue() < 3)
                {
                	System.out.print("Update tuple: " + tup + " to be: ");
                	tup.setField(1, new IntField(3));
                	System.out.println(tup);
                	Database.getBufferPool().deleteTuple(new TransactionId(), tup);
                	Database.getBufferPool().insertTuple(new TransactionId(), table1.getId(), tup);
                }
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
        
        Tuple t1 = new Tuple(descriptor);
        t1.setField(0, new IntField(99));
        t1.setField(1, new IntField(99));
        t1.setField(2, new IntField(99));
        System.out.println("Insert tuple: " + t1);
        try {
        	Database.getBufferPool().insertTuple(tid, table1.getId(),t1);
		} catch (DbException e1) {
		} catch (TransactionAbortedException e1) {
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        try {
			Database.getBufferPool().flushAllPages();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        try {
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("Tuple: " + tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
	}

}
