package simpledb.systemtest;

import junit.framework.Assert;
import org.junit.Test;
import simpledb.*;

import java.io.IOException;

import static junit.framework.Assert.fail;

/**
 * Test logging, aborts, and recovery.
 */
public class LogRollbackTest extends LogTestBase {

    @Test
    public void TestFlushAll()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        // *** Test:
        // check that flushAllPages writes the HeapFile

        Transaction t1 = new Transaction();
        t1.start();
        HeapPage pageBefore = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));
        insertRow(hf1, t1, 3, 0);
        Database.getBufferPool().flushAllPages();
        HeapPage pageAfter = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));

        if (pageBefore.getNumEmptySlots() == pageAfter.getNumEmptySlots()) {
            Assert.fail("LogTest: flushAllPages() had no effect");
        }
    }

    @Test
    public void TestRollback()
            throws IOException, DbException, TransactionAbortedException {

        setup();

        // *** Test:
        // rollback a txn

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);
        insertRow(hf1, t1, 4, 0);
        Database.getBufferPool().flushAllPages(); // ensure there's something to UNDO

        abort(t1);

        Transaction t3 = new Transaction();
        t3.start();
        look(hf1, t3, 3, false);                  // this should have been rolled back
        look(hf1, t3, 4, false);
        t3.commit();
    }

    @Test
    public void TestNoRollback()
            throws IOException, DbException, TransactionAbortedException {

        setup();

        // *** Test:
        // commit a txn and then try to rollback
        doInsert(hf1, 1, 2);  // some other txn modifies *different* table, adds to log

        Transaction t2 = new Transaction();
        t2.start();
        look(hf1, t2, 1, true);                  // should be there
        look(hf1, t2, 2, true);
        t2.commit();

        try {
            Database.getLogFile().logAbort(t2.getId());  // okay, this is a bit weird to do b/c txn committed...  nevertheless
            Assert.fail("Error!  Should not be able to able to abort a committed transaction!");
        } catch (IOException ignored) {}

        Transaction t3 = new Transaction();
        t3.start();
        look(hf1, t2, 1, true);                  // should still be there
        look(hf1, t2, 2, true);
        t3.commit();
    }


    @Test
    public void TestRollbackWithOthers()
            throws IOException, DbException, TransactionAbortedException {

        setup();

        // *** Test:
        // rollback a txn but other txns adding entries in between

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);
        Database.getBufferPool().flushAllPages(); // ensure there's something to UNDO
        doInsert(hf2, 1, 2);  // some other txn modifies *different* table, adds to log
        insertRow(hf1, t1, 4, 0);
        Database.getBufferPool().flushAllPages(); // again, something to UNDO
        abort(t1);

        Transaction t = new Transaction();
        t.start();
        look(hf2, t, 1, true);                   // this txn commits, don't UNDO it!
        look(hf2, t, 2, true);
        look(hf1, t, 3, false);                  // this should have been rolled back
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test
    public void TestRollbackWithCheckPoint()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        doInsert(hf2, 1, 2);

        // rollback a txn over a check point

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);
        Database.getLogFile().logCheckpoint();
        insertRow(hf1, t1, 4, 0);
        abort(t1);

        Transaction t = new Transaction();
        t.start();
        look(hf2, t, 1, true);
        look(hf2, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        t.commit();
    }

    @Test
    public void TestAbortCommitInterleaved()
            throws IOException, DbException, TransactionAbortedException {
        setup();
        doInsert(hf1, 1, 2);

        // *** Test:
        // T1 start, T2 start and commit, T1 abort

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 3, 0);

        Transaction t2 = new Transaction();
        t2.start();
        insertRow(hf2, t2, 21, 0);
        Database.getLogFile().logCheckpoint();
        insertRow(hf2, t2, 22, 0);
        t2.commit();

        insertRow(hf1, t1, 4, 0);
        abort(t1);

        Transaction t = new Transaction();
        t.start();
        look(hf1, t, 1, true);
        look(hf1, t, 2, true);
        look(hf1, t, 3, false);
        look(hf1, t, 4, false);
        look(hf2, t, 21, true);
        look(hf2, t, 22, true);
        t.commit();
    }

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(LogRollbackTest.class);
    }
}
