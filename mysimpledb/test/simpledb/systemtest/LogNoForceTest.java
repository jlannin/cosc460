package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.IOException;
import java.util.Arrays;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * User: mhay
 * Date: 11/16/14 10:51 AM
 */
public class LogNoForceTest extends LogTestBase {

    @Test
    public void testAllDirtySucceeds()
            throws IOException, DbException, TransactionAbortedException {
        // Allocate a file with ~10 pages of data
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 512 * 10, null, null);
        Database.resetBufferPool(1);

        // BEGIN TRANSACTION
        Transaction t = new Transaction();
        t.start();

        // Insert a new row
        TransactionTestUtil.insertRow(f, t);

        // Scanning the table must fail because it can't evict the dirty page
        try {
            TransactionTestUtil.findMagicTuple(f, t);
        } catch (DbException e) {
            fail("Expected scan to run be able to evict buffer pages");
        }
        t.commit();
    }

    @Test
    public void testNoForce() throws TransactionAbortedException, IOException, DbException {
        setup();

        HeapPage before = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));

        Transaction t1 = new Transaction();
        t1.start();
        insertRow(hf1, t1, 10, 0);
        insertRow(hf1, t1, 11, 0);
        t1.commit();

        HeapPage after = (HeapPage) hf1.readPage(new HeapPageId(hf1.getId(), 0));
        assertTrue("Should be the same provided commit doesn't flush",
                Arrays.equals(before.getPageData(), after.getPageData()));
    }

}
