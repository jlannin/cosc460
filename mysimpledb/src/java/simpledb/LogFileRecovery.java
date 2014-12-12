package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

	private final RandomAccessFile readOnlyLog;

	/**
	 * Helper class for LogFile during rollback and recovery.
	 * This class given a read only view of the actual log file.
	 *
	 * If this class wants to modify the log, it should do something
	 * like this:  Database.getLogFile().logAbort(tid);
	 *
	 * @param readOnlyLog a read only copy of the log file
	 */
	public LogFileRecovery(RandomAccessFile readOnlyLog) {
		this.readOnlyLog = readOnlyLog;
	}

	/**
	 * Print out a human readable representation of the log
	 */
	public void print() throws IOException {
		// since we don't know when print will be called, we can save our current location in the file
		// and then jump back to it after printing
		Long currentOffset = readOnlyLog.getFilePointer();

		readOnlyLog.seek(0);
		long lastCheckpoint = readOnlyLog.readLong(); // ignore this
		System.out.println("BEGIN LOG FILE");
		while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
			int type = readOnlyLog.readInt();
			long tid = readOnlyLog.readLong();
			switch (type) {
			case LogType.BEGIN_RECORD:
				System.out.println("<T_" + tid + " BEGIN>");
				break;
			case LogType.COMMIT_RECORD:
				System.out.println("<T_" + tid + " COMMIT>");
				break;
			case LogType.ABORT_RECORD:
				System.out.println("<T_" + tid + " ABORT>");
				break;
			case LogType.UPDATE_RECORD:
				Page beforeImg = LogFile.readPageData(readOnlyLog);
				Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
				System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
				break;
			case LogType.CLR_RECORD:
				afterImg = LogFile.readPageData(readOnlyLog);  // after image
				System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
				break;
			case LogType.CHECKPOINT_RECORD:
				int count = readOnlyLog.readInt();
				Set<Long> tids = new HashSet<Long>();
				for (int i = 0; i < count; i++) {
					long nextTid = readOnlyLog.readLong();
					tids.add(nextTid);
				}
				System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
				break;
			default:
				throw new RuntimeException("Unexpected type!  Type = " + type);
			}
			long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
		}
		System.out.println("END LOG FILE");

		// return the file pointer to its original position
		readOnlyLog.seek(currentOffset);

	}

	/**
	 * Rollback the specified transaction, setting the state of any
	 * of pages it updated to their pre-updated state.  To preserve
	 * transaction semantics, this should not be called on
	 * transactions that have already committed (though this may not
	 * be enforced by this method.)
	 *
	 * This is called from LogFile.recover after both the LogFile and
	 * the BufferPool are locked.
	 *
	 * @param tidToRollback The transaction to rollback
	 * @throws java.io.IOException if tidToRollback has already committed
	 */
	public void rollback(TransactionId tidToRollback) throws IOException {        
		/*
		 * When stopping: when get to transaction begin 
		 * think about when committed (if not there, if before checkpoint)
		 */
		boolean notBegin = true;
		long rollTid = tidToRollback.getId();
		long currLoc = readOnlyLog.length();
		long tid;
		int type;
		long startRecord;
		while(notBegin)
		{
			do
			{
				readOnlyLog.seek(currLoc-LogFile.LONG_SIZE);
				startRecord = readOnlyLog.readLong();
				currLoc = startRecord;
				readOnlyLog.seek(startRecord);
				type = readOnlyLog.readInt();
				tid = readOnlyLog.readLong();
			} while(rollTid != tid);

			//have a log record for this TransactionId

			switch (type) {
			case LogType.BEGIN_RECORD:
				Database.getLogFile().logAbort(tid);
				notBegin = false;
				break;
			case LogType.COMMIT_RECORD:
				throw new IOException("Transaction already comitted!");
			case LogType.UPDATE_RECORD:
				Page beforeImg = LogFile.readPageData(readOnlyLog);
				PageId pid = beforeImg.getId();
				DbFile dbdel = Database.getCatalog().getDatabaseFile(pid.getTableId());
				dbdel.writePage(beforeImg);
				Database.getBufferPool().discardPage(pid);
				Database.getLogFile().logCLR(tid, beforeImg);
				break;
			case LogType.ABORT_RECORD:
				throw new IOException("Transaction already aborted!");
			case LogType.CLR_RECORD:
				break;
			default:
				throw new RuntimeException("Unexpected type!  Type = " + type);    
			}
		}
	}

	/**
	 * Recover the database system by ensuring that the updates of
	 * committed transactions are installed and that the
	 * updates of uncommitted transactions are not installed.
	 *
	 * This is called from LogFile.recover after both the LogFile and
	 * the BufferPool are locked.
	 */
	public void recover() throws IOException {
		//start at checkpoint, do redo stage, redoing everything, then do undo stage, undoing everything
		readOnlyLog.seek(0);
		HashSet<Long> undoTids = new HashSet<Long>();
		long lastCheckpoint = readOnlyLog.readLong();
		if(lastCheckpoint != -1)
		{
			readOnlyLog.seek(lastCheckpoint);
			if(readOnlyLog.readInt() != LogType.CHECKPOINT_RECORD)
			{
				throw new IOException("Checkpoint is mislabeled!");
			}
			readOnlyLog.readLong(); //skip -1 Tid
			//get active transactions from checkpoint
			int count = readOnlyLog.readInt();
			for (int i = 0; i < count; i++) {
				long nextTid = readOnlyLog.readLong();
				undoTids.add(nextTid);
			}
			readOnlyLog.readLong();
		}

		//start the redo phase
		redoPhase(undoTids);
		undoPhase(undoTids);

	}


	/**
	 * This does the redo phase of recovery with
	 * undoTids being updated to contain the list of transactions that
	 * do not commit or abort and so will have to be undone.
	 * @param undoTids
	 * @throws IOException
	 */
	private void redoPhase(HashSet<Long> undoTids) throws IOException
	{
		while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
			int type = readOnlyLog.readInt();
			long tid = readOnlyLog.readLong();
			switch (type) {
			case LogType.BEGIN_RECORD:
				if(undoTids.contains(tid))
				{
					throw new IOException("Mulitple \"begin records\" for the same transaction");
				}
				undoTids.add(tid);
				break;
			case LogType.COMMIT_RECORD:
				if(!undoTids.contains(tid))
				{
					throw new IOException(tid + " already completed, cannot commit!");
				}
				undoTids.remove(tid);
				break;
			case LogType.ABORT_RECORD:
				if(!undoTids.contains(tid))
				{
					throw new IOException(tid + " already completed, cannot abort!");
				}
				undoTids.remove(tid);
				break;
			case LogType.UPDATE_RECORD:
				if(!undoTids.contains(tid))
				{
					throw new IOException(tid + " is not active! Where did this log record come from!?!");
				}
				Page beforeImg = LogFile.readPageData(readOnlyLog);
				Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
				PageId pid = beforeImg.getId();
				DbFile dbdel = Database.getCatalog().getDatabaseFile(pid.getTableId());
				dbdel.writePage(afterImg);

				break;
			case LogType.CLR_RECORD:
				if(!undoTids.contains(tid))
				{
					throw new IOException(tid + " is not active! Where did this log record come from!?!");
				}
				afterImg = LogFile.readPageData(readOnlyLog);  // after image
				pid = afterImg.getId();
				dbdel = Database.getCatalog().getDatabaseFile(pid.getTableId());
				dbdel.writePage(afterImg);
				break;
			default:
				throw new RuntimeException("Unexpected type!  Type = " + type);
			}
			long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
		}
	}

	/**
	 * This function undoes all of the transactions in undoTids.
	 * @param undoTids
	 * @throws IOException
	 */
	private void undoPhase(HashSet<Long> undoTids) throws IOException
	{
		long currLoc = readOnlyLog.length();
		long tid;
		int type;
		long startRecord;
		while(!undoTids.isEmpty())
		{
			do
			{
				readOnlyLog.seek(currLoc-LogFile.LONG_SIZE);
				startRecord = readOnlyLog.readLong();
				currLoc = startRecord;
				readOnlyLog.seek(startRecord);
				type = readOnlyLog.readInt();
				tid = readOnlyLog.readLong();
			} while(!undoTids.contains(tid));

			//have a log record in the undo list

			switch (type) {
			case LogType.BEGIN_RECORD:
				Database.getLogFile().logAbort(tid);
				undoTids.remove(tid);
				break;
			case LogType.COMMIT_RECORD:
				throw new IOException("Transaction already comitted, should not be undoing!");
			case LogType.UPDATE_RECORD:
				Page beforeImg = LogFile.readPageData(readOnlyLog);
				PageId pid = beforeImg.getId();
				DbFile dbdel = Database.getCatalog().getDatabaseFile(pid.getTableId());
				dbdel.writePage(beforeImg);
				Database.getBufferPool().discardPage(pid);
				Database.getLogFile().logCLR(tid, beforeImg);
				break;
			case LogType.ABORT_RECORD:
				throw new IOException("Transaction already aborted, should not be undoing!");
			case LogType.CLR_RECORD:
				break;
			default:
				throw new RuntimeException("Unexpected type!  Type = " + type);    
			}
		}
	}
}
