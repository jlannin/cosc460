package simpledb;

import java.io.IOException;
import java.util.List;

public class Lab5Util {

    /**
     * Starts a bunch of transactions simultaneously.
     * @param transactions
     */
    public static void runTransactions(SimpleDBTransactionThread... transactions) {
        for (SimpleDBTransactionThread transaction : transactions) {
            transaction.start();
        }
    }

    /**
     * Starts a bunch of transactions simultaneously.
     * @param transactions
     */
    public static void runTransactions(List<SimpleDBTransactionThread> transactions) {
        for (SimpleDBTransactionThread transaction : transactions) {
            transaction.start();
        }
    }
}

/**
 * Creates a thread, runs execute and then appropriately calls
 * BufferPool.transactionComplete when the transaction completes
 * (i.e., commits or aborts).
 *
 * Subclasses must implement execute to give transaction desired
 * behavior.
 */
abstract class SimpleDBTransactionThread extends Thread {
    protected final TransactionId tid = new TransactionId();

    @Override
    public void run() {
        try {
            System.out.println("SimpleDBTransactionThread: starting transaction " + tid);
            execute();
            System.out.println("SimpleDBTransactionThread: committing transaction " + tid);
            Database.getBufferPool().transactionComplete(tid, true);
        } catch (TransactionAbortedException e) {
            System.out.println("SimpleDBTransactionThread: aborting transaction " + tid);
            try {
                Database.getBufferPool().transactionComplete(tid, false);
            } catch (IOException e2) {
                e2.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void execute() throws TransactionAbortedException, DbException;
}