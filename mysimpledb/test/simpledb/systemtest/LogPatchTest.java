package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import static junit.framework.Assert.assertTrue;

/**
 * User: mhay
 * Date: 11/16/14 9:57 AM
 */
public class LogPatchTest extends LogTestBase {

    @Test
    public void testBeforeImageIsSet()
            throws IOException, DbException, TransactionAbortedException {
        setup();

        doInsert(hf1, 1, 2);  // inserts and commits

        // *** Test:
        // check that BufferPool.transactionComplete(commit=true)
        // calls Page.setBeforeImage().
        Transaction t1 = new Transaction();
        t1.start();
        Page p = Database.getBufferPool().getPage(t1.getId(),
                new HeapPageId(hf1.getId(), 0),
                Permissions.READ_ONLY);
        Page before = p.getBeforeImage();
        assertTrue("Before image should be set upon commit",
                Arrays.equals(p.getPageData(), before.getPageData()));
    }

}
