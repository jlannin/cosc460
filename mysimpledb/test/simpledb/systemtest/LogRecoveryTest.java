package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * User: mhay
 * Date: 11/14/14 4:17 PM
 */
public class LogRecoveryTest extends LogTestBase {

    @Test
    public void TestCommitCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // insert, crash, recover: data should still be there

        doInsert(hf1, 1, 2);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        t.commit();
    }

    @Test
    public void TestAbortCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        dontInsert(hf1, 4, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();

        // *** Test:
        // crash and recover: data should still not be there
        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test
    public void TestCommitAbortCommitCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts and commits
        // T2 inserts but aborts
        // T3 inserts and commit
        // only T1 and T3 data should be there

        doInsert(hf1, 5, -1);
        dontInsert(hf1, 6, -1);
        doInsert(hf1, 7, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 5, true);
        look(hf1, t, 6, false);
        look(hf1, t, 7, true);
        t.commit();

        // *** Test:
        // crash: should not change visible data

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        look(hf1, t, 5, true);
        look(hf1, t, 6, false);
        look(hf1, t, 7, true);
        t.commit();
    }

    @Test
    public void TestOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // insert but no commit
        // crash
        // data should not be there

        Transaction t = new Transaction();
        t.start();
        insertRow(hf1, t, 8, 0);
        Database.getBufferPool().flushAllPages(); // XXX something to UNDO
        insertRow(hf1, t, 9, 0);

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 8, false);
        look(hf1, t, 9, false);
        t.commit();
    }

    @Test
    public void TestOpenCommitOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts but does not commit
        // T2 inserts and commits
        // T3 inserts but does not commit
        // crash
        // only T2 data should be there

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 10, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf1, t1, 11, 0);

        // T2 commits
        doInsert(hf2, 22, 23);

        Transaction t3 = new Transaction();
        t3.start();
        insertRow(hf2, t3, 24, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf2, t3, 25, 0);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 10, false);
        look(hf1, t, 11, false);
        look(hf2, t, 22, true);
        look(hf2, t, 23, true);
        look(hf2, t, 24, false);
        look(hf2, t, 25, false);
        t.commit();
    }

    @Test
    public void TestOpenCommitCheckpointOpenCrash()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 inserts but does not commit
        // T2 inserts and commits
        // checkpoint
        // T3 inserts but does not commit
        // crash
        // only T2 data should be there

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 12, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf1, t1, 13, 0);

        // T2 commits
        doInsert(hf2, 26, 27);

        Database.getLogFile().logCheckpoint();

        Transaction t3 = new Transaction();
        t3.start();
        insertRow(hf2, t3, 28, 0);
        Database.getBufferPool().flushAllPages(); // XXX defeat NO-STEAL-based abort
        insertRow(hf2, t3, 29, 0);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 12, false);
        look(hf1, t, 13, false);
        look(hf2, t, 22, false);
        look(hf2, t, 23, false);
        look(hf2, t, 24, false);
        look(hf2, t, 25, false);
        look(hf2, t, 26, true);
        look(hf2, t, 27, true);
        look(hf2, t, 28, false);
        look(hf2, t, 29, false);
        t.commit();
    }

    @Test
    public void TestRepeatHistoryEvenAborts()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // crash and recover: recovery should replay history
        // including rolling back the abort

        // note: could pass this test by simply repeating history
        // then undoing all aborts.  second version of this test
        // should catch that.

        doInsert(hf1, 1, 2);

        dontInsert(hf1, 3, 4);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test
    public void TestRepeatHistoryEvenAborts2()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // T0 modifies first table and commits
        // T1 modifies both tables
        // T1 aborts...  should rollback changes and add CLRs
        // T2 modifies second table
        // crash should redo history (including T1 abort) and T2's
        // later commits

        doInsert(hf1, 1, 2);

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf2, t1, 30, 0);
        insertRow(hf1, t1, 3, 0);
        abort(t1);

        doInsert(hf2, 20, -1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf2, t, 30, false);
        look(hf2, t, 20, true);
        t.commit();

        crash();

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf2, t, 30, false);
        look(hf2, t, 20, true);          // if recovery undoes all of T2 change, then this will not show
        t.commit();
    }

    @Test
    public void TestRepeatHistoryEvenRecovery()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // T0 inserts and commits
        // T1 inserts
        // flush T1's log update record to disk, then crash
        // recovery should undo T1 and write a CLR and ABORT for T1
        // T2 inserts and commits
        // crash again and make sure that T1 is not there but T2 is

        doInsert(hf1, 1, -1);

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);
        Database.getBufferPool().flushAllPages(); // ensure there's something to UNDO

        crash();              // recovery will rollback T1

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 3, false);
        t.commit();

        doInsert(hf1, 4, -1);   // T3 inserts and commits

        crash();             // recovery should replay history, including rollback of T1

        t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, true);        // if recovery undoes all of T2 change, then this will not show
        t.commit();
    }

    @Test
    public void checkpointTest() throws TransactionAbortedException, IOException, DbException {
        setup();
        // *** Test
        // T1 commits,
        // log check point
        // then hack hf1 and insert tuple directly (bypassing logging system)
        // T2 commits
        // when redo *starting* at checkpoint, should not see T1
        doInsert(hf1, 1, -1);

        Database.getLogFile().logCheckpoint();

        Transaction t1 = new Transaction();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(t1.getId(),
                new HeapPageId(hf1.getId(), 0),
                Permissions.READ_ONLY);
        t1.commit();

        // make copy of page
        int v1 = 3;
        int v2 = 4;
        p = new HeapPage(p.getId(), p.getPageData());
        Iterator<Tuple> iterator = p.iterator();
        List<Tuple> tempList = new LinkedList<Tuple>();
        while (iterator.hasNext()) {
            tempList.add(iterator.next());
        }
        for (Tuple tuple : tempList) {
            p.deleteTuple(tuple);
        }
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(v1));
        value.setField(1, new IntField(v2));
        p.insertTuple(value);
        hf1.writePage(p);   // total hack: write the page by passing

        doInsert(hf2, 30, 40);

        crash();

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, false);
        look(hf1, t, 3, true);
        look(hf2, t, 30, true);
        look(hf2, t, 40, true);
        t.commit();

    }
}
