package simpledb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import junit.framework.JUnit4TestAdapter;
import simpledb.systemtest.SystemTestUtil;

public class LockingTest extends TestUtil.CreateHeapFile {
    private PageId p0, p1;
    private TransactionId tid1, tid2;

    /**
     * Time to wait before checking the state of lock contention, in ms
     */
    private static final int TIMEOUT = 100;

    // just so we have a pointer shorter than Database.getBufferPool()
    private BufferPool bp;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();

        // clear all state from the buffer pool
        bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);

        TransactionId tid = new TransactionId();
        empty = SystemTestUtil.createRandomHeapFile(2, 1025, null, null);

        assertEquals(3, empty.numPages());

        this.p0 = new HeapPageId(empty.getId(), 0);
        this.p1 = new HeapPageId(empty.getId(), 1);
        this.tid1 = new TransactionId();
        this.tid2 = new TransactionId();

        // forget about locks associated to tid, so they don't conflict with
        // test cases
        bp.getPage(tid, p0, Permissions.READ_WRITE).markDirty(true, tid);
        bp.getPage(tid, p1, Permissions.READ_WRITE).markDirty(true, tid);
        bp.flushAllPages();
        bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
    }

    /**
     * Generic unit test structure for BufferPool.getPage() assuming locking.
     *
     * @param tid1     the first transaction Id
     * @param pid1     the first page to lock over
     * @param perm1    the type of lock for the first page
     * @param tid2     the second transaction Id
     * @param pid2     the second page to lock over
     * @param perm2    the type of lock for the second page
     * @param expected true if we expect the second acquisition to succeed;
     *                 false otherwise
     */
    public void metaLockTester(
            TransactionId tid1, PageId pid1, Permissions perm1,
            TransactionId tid2, PageId pid2, Permissions perm2,
            boolean expected) throws Exception {

        bp.getPage(tid1, pid1, perm1);
        grabLock(tid2, pid2, perm2, expected);
    }

    /**
     * Generic unit test structure to grab an additional lock in a new
     * thread.
     *
     * @param tid      the transaction Id
     * @param pid      the first page to lock over
     * @param perm     the type of lock desired
     * @param expected true if we expect the acquisition to succeed;
     *                 false otherwise
     */
    public void grabLock(TransactionId tid, PageId pid, Permissions perm,
                         boolean expected) throws Exception {

        TestUtil.LockGrabber t = new TestUtil.LockGrabber(tid, pid, perm);
        t.start();

        // if we don't have the lock after TIMEOUT, we assume blocking.
        Thread.sleep(TIMEOUT);
        assertEquals(expected, t.acquired());
        assertNull(t.getError());

        // yes, stop() is evil, but this is unit test cleanup
        t.stop();
    }

    /**
     * Unit test for BufferPool.getPage() assuming locking.
     * Acquires a read lock and a write lock on the same page, in that order.
     */
    @Test
    public void acquireWriteLocksOnSamePage() throws Exception {
        metaLockTester(tid1, p0, Permissions.READ_WRITE,
                tid2, p0, Permissions.READ_WRITE, false);
    }

    /**
     * Unit test for BufferPool.getPage() assuming locking.
     * Acquires write locks on different pages.
     */
    @Test
    public void acquireWriteLocksOnTwoPages() throws Exception {
        metaLockTester(tid1, p0, Permissions.READ_WRITE,
                tid2, p1, Permissions.READ_WRITE, true);
    }

    /**
     * Unit test for BufferPool.getPage() and BufferPool.releasePage()
     * assuming locking.
     * Acquires read locks on different pages.
     */
    @Test
    public void acquireThenRelease() throws Exception {
        bp.getPage(tid1, p0, Permissions.READ_WRITE);
        bp.releasePage(tid1, p0);
        grabLock(tid2, p0, Permissions.READ_WRITE, true);

        bp.getPage(tid2, p1, Permissions.READ_WRITE);
        bp.releasePage(tid2, p1);
        grabLock(tid1, p1, Permissions.READ_WRITE, true);
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(LockingTest.class);
    }

}

