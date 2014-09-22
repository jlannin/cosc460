package simpledb;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Lab1Main {

	public static void main(String[] argv) {
		
		//System.out.println(Integer.toBinaryString(header[1]));
		
		  // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{ "field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        TupleDesc des1 = new TupleDesc(types, names);
        //System.out.println(TupleDesc.merge(descriptor,  des1)); used for testing

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
       
		
        try {
            // and run it
            f.open();
            int count = 0;
            boolean repeat = true;
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
                count++;
                if (repeat && count == 200)
                {
                	repeat = false;
                	//f.rewind();  used for testing
                }
            }
            //System.out.println(count); used for testing
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
        
    }

		
		
	
}