package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test logging, aborts, and recovery.
 */
public class LogTestBase extends SimpleDbTestBase {
    File file1;
    File file2;
    HeapFile hf1;
    HeapFile hf2;

    void insertRow(HeapFile hf, Transaction t, int v1, int v2)
            throws DbException, TransactionAbortedException {
        // Create a row to insert
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(v1));
        value.setField(1, new IntField(v2));
        TupleIterator insertRow = new TupleIterator(Utility.getTupleDesc(2), Arrays.asList(new Tuple[]{value}));

        // Insert the row
        Insert insert = new Insert(t.getId(), insertRow, hf.getId());
        insert.open();
        Tuple result = insert.next();
        assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, result.getTupleDesc());
        assertEquals(1, ((IntField) result.getField(0)).getValue());
        assertFalse(insert.hasNext());
        insert.close();
    }

    // check that the specified tuple is, or is not, present
    void look(HeapFile hf, Transaction t, int v1, boolean present)
            throws DbException, TransactionAbortedException {
        int count = 0;
        SeqScan scan = new SeqScan(t.getId(), hf.getId(), "");
        scan.open();
        while (scan.hasNext()) {
            Tuple tu = scan.next();
            int x = ((IntField) tu.getField(0)).getValue();
            if (x == v1)
                count = count + 1;
        }
        scan.close();
        if (count > 1)
            throw new RuntimeException("LogTest: tuple repeated");
        if (present && count < 1)
            throw new RuntimeException("LogTest: tuple missing");
        if (present == false && count > 0)
            throw new RuntimeException("LogTest: tuple present but shouldn't be");
    }

    // insert tuples
    void doInsert(HeapFile hf, int t1, int t2)
            throws DbException, TransactionAbortedException, IOException {
        Transaction t = new Transaction();
        t.start();
        if (t1 != -1)
            insertRow(hf, t, t1, 0);
        Database.getBufferPool().flushAllPages();
        if (t2 != -1)
            insertRow(hf, t, t2, 0);
        Database.getBufferPool().flushAllPages();
        t.commit();
    }

    void abort(Transaction t)
            throws DbException, TransactionAbortedException, IOException {
        // t.transactionComplete(true); // abort
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        Database.getLogFile().logAbort(t.getId()); // does rollback too
        Database.getBufferPool().flushAllPages(); // prevent NO-STEAL-based abort from
        // un-doing the rollback
        Database.getBufferPool().transactionComplete(t.getId(), false); // release locks
    }

    // insert tuples
    // force dirty pages to disk, defeating NO-STEAL
    // abort
    void dontInsert(HeapFile hf, int t1, int t2)
            throws DbException, TransactionAbortedException, IOException {
        Transaction t = new Transaction();
        t.start();
        if (t1 != -1)
            insertRow(hf, t, t1, 0);
        if (t2 != -1)
            insertRow(hf, t, t2, 0);
        if (t1 != -1)
            look(hf, t, t1, true);
        if (t2 != -1)
            look(hf, t, t2, true);
        abort(t);
    }

    // simulate crash
    // restart Database
    // run log recovery
    void crash()
            throws DbException, TransactionAbortedException, IOException {
        Database.reset();
        hf1 = Utility.openHeapFile(2, file1);
        hf2 = Utility.openHeapFile(2, file2);
        Database.getLogFile().recover();
    }

    // create an initial database with two empty tables
    // does *not* initiate log file recovery
    void setup()
            throws IOException, DbException, TransactionAbortedException {
        Database.reset();

        // empty heap files w/ 2 columns.
        // adds to the catalog.
        file1 = new File("simple1.db");
        file1.delete();
        file2 = new File("simple2.db");
        file2.delete();
        hf1 = Utility.createEmptyHeapFile(file1.getAbsolutePath(), 2);
        hf2 = Utility.createEmptyHeapFile(file2.getAbsolutePath(), 2);
    }
}
