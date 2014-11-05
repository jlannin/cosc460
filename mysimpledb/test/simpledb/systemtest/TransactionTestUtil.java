package simpledb.systemtest;

import simpledb.*;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TransactionTestUtil {
    public static void insertRow(HeapFile f, Transaction t) throws DbException,
            TransactionAbortedException {
        // Create a row to insert
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(-42));
        value.setField(1, new IntField(-43));
        TupleIterator insertRow = new TupleIterator(Utility.getTupleDesc(2), Arrays.asList(new Tuple[]{value}));

        // Insert the row
        Insert insert = new Insert(t.getId(), insertRow, f.getId());
        insert.open();
        Tuple result = insert.next();
        assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, result.getTupleDesc());
        assertEquals(1, ((IntField) result.getField(0)).getValue());
        assertFalse(insert.hasNext());
        insert.close();
    }

    public static boolean findMagicTuple(HeapFile f, Transaction t)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(t.getId(), f.getId(), "");
        boolean found = false;
        ss.open();
        while (ss.hasNext()) {
            Tuple v = ss.next();
            int v0 = ((IntField) v.getField(0)).getValue();
            int v1 = ((IntField) v.getField(1)).getValue();
            if (v0 == -42 && v1 == -43) {
                assertFalse(found);
                found = true;
            }
        }
        ss.close();
        return found;
    }
}
