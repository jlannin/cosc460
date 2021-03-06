package simpledb;

import java.io.IOException;

/**
 * A very simple main method that can be used to test concurrency and locking.
 * Basically, create as many "transactions" as you want by subclassing
 * SimpleDBThread and overwriting the execute method (see T1 and T2 below).
 * Then set these transactions running simultaneously.
 *
 * @see Lab5Util
 */
public class Lab5Main {

    public static void main(String[] args) {
        System.out.println("Loading schema from file:");
        // loads the imdb database because each table is big enough to have multiple pages
        Database.getCatalog().loadSchema("imdb.schema");             // file imdb.schema must be in mysimpledb directory

        Lab5Util.runTransactions(new T1());
    }

    static class T1 extends SimpleDBTransactionThread {

        @Override
        protected void execute() throws TransactionAbortedException, DbException {
            int table = Database.getCatalog().getTableId("Actor");
            t.start();
            PageId p0 = new HeapPageId(table, 0);
            PageId p1 = new HeapPageId(table, 1);
            System.out.println("GettingLock");
            HeapPage test = (HeapPage)Database.getBufferPool().getPage(t.getId(), p0, Permissions.READ_ONLY);
            test.markDirty(true, t.getId());
            try {
				t.commit();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //System.out.println("got both locks " + tid);
       //     Database.getBufferPool().releasePage(tid, p0);
         //   System.out.println("Released!" + tid);
            
            //Database.getBufferPool().getPage(tid, p0, Permissions.READ_WRITE);
            //System.out.println("got both locks " + tid);
            /*try {
                Thread.sleep(5);              // pause to encourage deadlock
            } catch (InterruptedException ignored) { }
            Database.getBufferPool().getPage(tid, p1, Permissions.READ_WRITE);
            */
           // System.out.println("got both locks " + tid);
        }
    }

    static class T2 extends SimpleDBTransactionThread {

        @Override
        protected void execute() throws TransactionAbortedException, DbException {
            int table = Database.getCatalog().getTableId("Actor");
            PageId p0 = new HeapPageId(table, 0);
            PageId p1 = new HeapPageId(table, 1);
            System.out.println("I am" + t);
            Database.getBufferPool().getPage(t.getId(), p0, Permissions.READ_WRITE);       // creates deadlock w/ T1!
            /*try {
                Thread.sleep(5);             // pause to encourage deadlock
            } catch (InterruptedException ignored) { }
            Database.getBufferPool().getPage(tid, p0, Permissions.READ_WRITE);
            */
            System.out.println("got both locks " + t);
            //Database.getBufferPool().releasePage(tid, p0);
           //7 System.out.println("Released!");
            //Database.getBufferPool().releasePage(tid,  p0);
        }
    }

    static class T3 extends SimpleDBTransactionThread {

        @Override
        protected void execute() throws TransactionAbortedException, DbException {
            int table = Database.getCatalog().getTableId("Actor");
            PageId p0 = new HeapPageId(table, 0);
            PageId p1 = new HeapPageId(table, 1);
            System.out.println(t + "RW");
            Database.getBufferPool().getPage(t.getId(), p0, Permissions.READ_ONLY);       // creates deadlock w/ T1!
            /*try {
                Thread.sleep(5);             // pause to encourage deadlock
            } catch (InterruptedException ignored) { }
            Database.getBufferPool().getPage(tid, p0, Permissions.READ_WRITE);
            */
            System.out.println("got both locks " + t);
            Database.getBufferPool().releasePage(t.getId(), p0);
            System.out.println("Released!" + t);
            //Database.getBufferPool().releasePage(tid,  p0);
        }
    }
}
