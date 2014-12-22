package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/**
	 * Bytes per page, including header.
	 */
	public static final int PAGE_SIZE = 4096;

	private static int pageSize = PAGE_SIZE;

	private final boolean FORCE = false;
	private final boolean NOSTEAL = false;

	/**
	 * Default number of pages passed to the constructor. This is used by
	 * other classes. BufferPool should use the numPages argument to the
	 * constructor instead.
	 */
	public static final int DEFAULT_PAGES = 50;
	private int numpages;
	private int pagespresent;
	ConcurrentHashMap<PageId, Page> idtopage;
	ConcurrentHashMap<PageId, Long> idtotime;
	LockTable lockTable;




	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		if (numPages < 0)
		{
			throw new RuntimeException();
		}
		numpages = numPages;
		pagespresent = 0;
		idtopage = new ConcurrentHashMap<PageId, Page>();
		idtotime = new ConcurrentHashMap<PageId, Long>();
		lockTable = new LockTable();

	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p/>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		lockTable.acquireLock(tid, pid, perm);
		synchronized(lockTable)
		{
			if(idtopage.containsKey(pid))
			{
				idtotime.put(pid, new Long(System.currentTimeMillis()));
				return idtopage.get(pid);
			}
			else
			{
				if (pagespresent == numpages)
				{
					evictPage();
				}
				int tableid = pid.getTableId();
				Catalog cat = Database.getCatalog();
				DbFile dbfile = cat.getDatabaseFile(tableid);
				Page newpage = dbfile.readPage(pid);
				idtopage.put(pid, newpage);
				idtotime.put(pid, new Long(System.currentTimeMillis()));
				pagespresent++;
				return newpage;
			}
		}
	}



	/**
	 * Releases the lock on a page.
	 * Calling this is very risky, and may result in wrong behavior. Think hard
	 * about who needs to call this and why, and why they can run the risk of
	 * calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		synchronized (lockTable) {

			//Update heldLocks
			LockNode ln = lockTable.tidLocks.get(tid);
			if(ln != null)
			{
				ln.releaseLocks(pid);
			}

			// update lockEntries
			LockRequest lr = lockTable.tableEntries.get(pid);
			boolean onFirst = true;
			while(lr != null)
			{
				if(onFirst && lr.getTransactionId().equals(tid)) //first entry
				{
					if(lr.next() == null) //no more entries so remove whole thing
					{
						lockTable.tableEntries.remove(pid);
					}
					else
					{
						lockTable.tableEntries.put(pid, lr.next()); //more entries so set second entry to be first entry
					}
					lr = lr.next();
				}
				else
				{
					onFirst = false; //now past first entry
					if(lr.next == null)//if no next we are done
					{
						break;
					}
					boolean end = false;
					while(!lr.next().getTransactionId().equals(tid))//searching for case when next LockRequest matches tid
					{
						lr = lr.next();
						if(lr.next == null)//no more LockRequests
						{
							end = true;
							break;
						}	
					}
					if(end)
					{
						break;
					}
					lr.setNext(lr.next().next());
				}
			}
		}
	}


	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */


	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		if(commit)
		{
			if(FORCE)
			{
				flushPages(tid); //flush all pages marked dirty by this transaction
			}
			transCompleteUpdateBeforeImg(tid); //handle log before image updating
		}
		else
		{
			undo(tid); //discard all pages marked dirty by this transaction
		}
		releaseLocks(tid); //release all locks that the tid has and is waiting for
	}

	/**
	 * Set before image to be current after image of all pages that tid
	 * held a lock on.
	 * @param tid
	 * @throws IOException
	 */
	public synchronized void transCompleteUpdateBeforeImg(TransactionId tid) throws IOException
	{
		LockNode node = lockTable.tidLocks.get(tid);
		if(node != null)
		{
			Iterator<PageId> iter = node.heldIter();
			while(iter.hasNext())
			{
				PageId key = iter.next();
				if (idtopage.containsKey(key))
				{
					Page flpage = idtopage.get(key);
					if (flpage == null)
					{
						throw new IOException("Page is null!"); 
					}

					flpage.setBeforeImage();
				}
			}
			// use current page contents as the before-image
			// for the next transaction that modifies this page.
		}

	}

	private synchronized void releaseLocks(TransactionId tid)
	{
		LockNode node = lockTable.tidLocks.get(tid);
		if(node != null)
		{
			//release waiting
			PageId waitingPid = node.getWaiting();
			releasePage(tid, waitingPid);
			//release held
			Iterator<PageId> iter = node.heldIter();
			releaseHelp(iter, tid);
			lockTable.tidLocks.remove(tid);
		}
	}

	private synchronized void releaseHelp(Iterator<PageId> iter, TransactionId tid)
	{
		HashSet<PageId> pidsToRemove = new HashSet<PageId>();
		while(iter.hasNext())
		{
			pidsToRemove.add(iter.next());

		}
		iter = pidsToRemove.iterator();
		while(iter.hasNext())
		{
			releasePage(tid,iter.next());
		}
	}

	private synchronized void undo(TransactionId tid)
	{
		Iterator<PageId> iter = (idtopage.keySet()).iterator();
		while(iter.hasNext())
		{
			PageId key = iter.next();
			if(tid.equals(idtopage.get(key).isDirty()))
			{
				discardPage(key);
			}
		}
	}

	/**
	 * We have the lock on BP and on the LogFile when we enter this
	 * method.  Writes all changes to the log before we add a commit and
	 * if using force then flush pages to disk.
	 * 
	 * @param tid
	 * @throws IOException
	 */
	public void updateLogBeforeCommit(TransactionId tid) throws IOException
	{
		//any pages not in BP at this point have already been added to log

		LockNode node = lockTable.tidLocks.get(tid);
		Iterator<PageId> iter = node.heldIter();
		while(iter.hasNext())
		{
			PageId key = iter.next();
			Page flpage = idtopage.get(key);
			if(flpage != null && ((HeapPage)flpage).logDirty()) //only if this page hasn't already been written to the log
			{
				Database.getLogFile().logWrite(tid, flpage.getBeforeImage(), flpage);
				Database.getLogFile().force();
				//here need to set log not dirty
				((HeapPage)flpage).markLogDirty(false);
			} 
		}
		if(FORCE)
		{
			flushPages(tid);
		}
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
	 * May block if the lock(s) cannot be acquired.
	 * <p/>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		if (t == null)
		{
			System.err.println("Tuple is null, aborting transaction!");
			throw new TransactionAbortedException();
		}
		DbFile dbdel = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> pages = dbdel.insertTuple(tid, t);
		insertInPool(pages, tid);
	}

	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 * <p/>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		if (t == null)
		{
			System.err.println("Tuple is null, aborting transaction!");
			throw new TransactionAbortedException();
		}
		RecordId rec = t.getRecordId();
		PageId delpageid = rec.getPageId();
		DbFile dbdel = Database.getCatalog().getDatabaseFile(delpageid.getTableId());
		ArrayList<Page> pages = dbdel.deleteTuple(tid, t);
		insertInPool(pages, tid);

	}

	private void insertInPool(ArrayList<Page> pages, TransactionId tid) throws DbException
	{
		synchronized(this)
		{
			for (int i = 0; i < pages.size(); i ++)
			{
				if (pagespresent == numpages)
				{
					evictPage();
				}
				Page dirpage = pages.get(i);
				dirpage.markDirty(true, tid);
				PageId dirid = dirpage.getId();
				if(!idtopage.containsKey(dirid))
				{
					pagespresent++;
				}
				idtopage.put(dirid, dirpage);
				idtotime.put(dirid, new Long(System.currentTimeMillis()));

			}
		}
	}

	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 * break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		Iterator<PageId> iter = (idtopage.keySet()).iterator();
		while (iter.hasNext())
		{
			flushPage(iter.next());
		}
	}

	/**
	 * Remove the specific page id from the buffer pool.
	 * Needed by the recovery manager to ensure that the
	 * buffer pool doesn't keep a rolled back page in its
	 * cache.
	 */
	public synchronized void discardPage(PageId pid) {
		idtotime.remove(pid);
		idtopage.remove(pid);
		numpages--;
	}

	/**
	 * Flushes a certain page to disk
	 *
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		if (pid == null)
		{
			throw new IOException("PageId is null!");
		}
		synchronized(this)
		{

			if (!idtopage.containsKey(pid))
			{
				throw new IOException("Page not in buffer!");
			}
			DbFile dbdel = Database.getCatalog().getDatabaseFile(pid.getTableId());
			Page flpage = idtopage.get(pid);

			if (flpage == null)
			{
				throw new IOException("Page is null!"); 
			}
			TransactionId dirtier;
			dirtier = flpage.isDirty();
			if(dirtier != null)
			{
				// append an update record to the log, with 
				// a before-image and after-image.
				if(((HeapPage)flpage).logDirty()) //only if this page hasn't already been written to the log
				{
					Database.getLogFile().logWrite(dirtier, flpage.getBeforeImage(), flpage);
					Database.getLogFile().force();
					//here need to set log not dirty
					((HeapPage)flpage).markLogDirty(false);
				} 
				dbdel.writePage(flpage);
				flpage.markDirty(false, null);
			}
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		Iterator<PageId> iter = (idtopage.keySet()).iterator();
		while(iter.hasNext())
		{
			PageId key = iter.next();
			if(tid.equals(idtopage.get(key).isDirty()))
			{
				flushPage(key);
			}
		}
	}

	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		synchronized(this)
		{
			Iterator<PageId> iter = (idtotime.keySet()).iterator();
			if (pagespresent > 0)
			{
				PageId key = iter.next();
				PageId minkey = key;
				long mintime = Long.MAX_VALUE;
				boolean found = false;
				long temptime = idtotime.get(key);
				if(NOSTEAL) //if in nosteal mode, cannot evict dirty pages
				{
					if (temptime < mintime && idtopage.get(key).isDirty() == null)
					{
						mintime = temptime;
						minkey = key;
						found = true;
					}
					while (iter.hasNext())
					{
						key = iter.next();
						temptime = idtotime.get(key);
						if (temptime < mintime && idtopage.get(key).isDirty() == null)
						{
							mintime = temptime;
							minkey = key;
							found = true;
						}
					}
				}
				else
				{
					if (temptime < mintime)
					{
						mintime = temptime;
						minkey = key;
						found = true;
					}
					while (iter.hasNext())
					{
						key = iter.next();
						temptime = idtotime.get(key);
						if (temptime < mintime)
						{
							mintime = temptime;
							minkey = key;
							found = true;
						}
					}
				}
				if (!found)
				{
					throw new DbException("Using No Steal: All pages dirty!");
				}
				else{
					try {
						flushPage(minkey);
					} catch (IOException e) {
						System.err.println(e.getMessage());
					}

					idtotime.remove(minkey);
					idtopage.remove(minkey);
					pagespresent--;
				}
			}
		}
	}

	class LockTable
	{
		HashMap<PageId, LockRequest> tableEntries;
		HashMap<TransactionId, LockNode> tidLocks;

		private static final int MAXWAIT = 100; //max amount waiting time if doing timeout based deadlock detection
		private static final int WAITINCREMENT = 10; //how long should each transaction wait before checking again?
		private static final boolean GRAPH = false; //True = do graph based deadlock detection, False = timeout based deadlock detection

		public LockTable()
		{
			tableEntries = new HashMap<PageId, LockRequest>();
			tidLocks = new HashMap<TransactionId, LockNode>();
		}

		/**
		 * The acquireLock method first handles whether we are upgrading
		 * or acquiring a new lock, adding a new LockRequest to lockTable.tableEntries,
		 * and assigns the lock right away if we can.  It then adds a new LockNode to 
		 * lockTable.tidLocks (after possibly checking for deadlock with a waits for graph). 
		 * Finally, it waits (if still necessary) and checks to see if we can acquire lock.
		 * @param tid
		 * @param pid
		 * @param perm
		 * @throws TransactionAbortedException
		 */
		public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException
		{
			//System.out.println(tid + " wants " + perm + " lock on " + pid);
			boolean waiting = true;
			//does this transaction already hold the lock?
			if(holdsLock(tid, pid))
			{
				waiting = handleLockUpgrade(tid, pid, perm);
			}
			else //transaction doesn't already hold lock
			{
				waiting = createLockRequest(tid, pid, perm);
			}

			//now lets start waiting for the lock

			if(waiting) //first we should add a waiting request to tidLocks
			{
				createWaitingLockNode(tid, pid, perm);
			}
			int secondsWaited = 0;
			int deadLock = MAXWAIT;

			//need to add to end of the heldlocks table
			//if read only then we need to make sure that every request before this one
			//is also a shared lock and has been granted
			while (waiting)
			{
				if(perm.equals(Permissions.READ_ONLY))
				{
					waiting = checkRead(tid, pid);
				}
				else
				{
					waiting = checkReadWrite(tid, pid);
				}
				if (waiting) {
					if(!GRAPH && secondsWaited == deadLock)
					{
						throw new TransactionAbortedException();
					}
					try {
						//System.out.println(tid + " Sleeping");
						Thread.sleep(WAITINCREMENT);
						secondsWaited += WAITINCREMENT;
					} catch (InterruptedException ignored) { }
				}
			}
			//System.out.println(tid + " Acquired " + perm + " Lock on " + pid);
			//System.out.print(tid + " Lock Table: ");
			//lockTable.printLockRequests(pid);
			//System.out.println();
		}

		/**
		 * This method handles all lock upgrades when tid already
		 * has a read lock on pid.
		 * @param tid
		 * @param pid
		 * @param perm
		 * @return
		 */
		private synchronized boolean handleLockUpgrade(TransactionId tid, PageId pid, Permissions perm)
		{
			if(perm.equals(Permissions.READ_ONLY)) //no need to upgrade when requesting read only lock
			{
				return false;
			}
			else //read/write requested so we might need to upgrade
			{
				LockRequest req = lockTable.tableEntries.get(pid);
				//if this transaction has the first lock, then it is possible to 
				//upgrade now
				if(req.getTransactionId().equals(tid))
				{
					if(req.shared())  //currently have read only lock, want read/write so need to upgrade
					{
						if(req.next() == null || !req.next().granted()) //can grant if nothing after or no others granted
						{
							req.setShared(perm);
							return false;
						}
					}
					else //already have read/write lock so we are done
					{
						return false;
					}
				}
				while(req.next() != null && req.next().granted() != false) //find spot before requests without lock
				{
					req = req.next();
				}
				LockRequest newreq = new LockRequest(tid, false, perm);
				newreq.setNext(req.next());
				req.setNext(newreq);
				return true;
			}
		}

		/**
		 * Creates a new lockRequest in lockTable.tableEntries.
		 * 
		 * If we can assign the lock right away (no locks held on pid)
		 * or we discover that we can acquire lock when we are looking through
		 * to determine where to add our new request, we acquire lock here.
		 * 
		 * return True = still waiting (did not acquire lock)
		 * return False = not waiting (acquired lock)
		 * @param tid
		 * @param pid
		 * @param perm
		 * @return
		 */
		private synchronized boolean createLockRequest(TransactionId tid, PageId pid, Permissions perm)
		{
			//check if no locks are currently held on page
			if(!lockTable.tableEntries.containsKey(pid)) //no locks on page, we can immediately grant it
			{
				lockTable.tableEntries.put(pid, new LockRequest(tid, true, perm));
				createLockNode(tid, pid);
				return false;
			}
			//locks are held on the page
			else
			{
				boolean allgranted = true;
				boolean noexclusive = true;
				LockRequest lr = lockTable.tableEntries.get(pid);
				LockRequest prev = lr;
				//the basic idea here is that if all of the locks held on the
				//page so far have been granted and are read only (shared),
				//then we can assign a read lock right away
				while(lr != null)
				{
					if(!lr.shared())
					{
						noexclusive = false;
					}
					if(!lr.granted())
					{
						allgranted = false;
					}
					prev = lr;
					lr = lr.next();
				}
				prev.setNext(new LockRequest(tid, false, perm)); //set not granted by default
				//if asking for a read only lock, all have been granted, and all are read only
				//then we can assign the lock
				//System.out.println(tid + " " + allgranted + "  " + noexclusive + " " + perm.equals(Permissions.READ_ONLY));
				//System.out.print(tid + " Lock Table1: ");
				//lockTable.printLockRequests(pid);
				if (allgranted && noexclusive && perm.equals(Permissions.READ_ONLY))
				{
					prev.next().setGranted(true);
					createLockNode(tid, pid);
					return false;
				}	
				return true;
			}
		}


		/**
		 * Check to see if tid can acquire the read lock.
		 * return True if cannot and are still waiting
		 * return False if we acquired the lock and have finished waiting
		 * @param tid
		 * @param pid
		 * @return
		 */
		private synchronized boolean checkRead(TransactionId tid, PageId pid)
		{
			boolean allgranted = true;
			boolean noexclusive = true;
			LockRequest req = lockTable.tableEntries.get(pid);
			noexclusive = true;
			allgranted = true;
			while(noexclusive && !req.getTransactionId().equals(tid))
			{
				if(!req.granted())
				{
					allgranted = false;
				}
				if(!req.shared())
				{
					noexclusive = false;
				}
				req = req.next();
			}
			if (allgranted && noexclusive) //if all previously granted and read only
			{
				req.setGranted(true);
				LockNode ln = lockTable.tidLocks.get(tid);
				ln.finishWaiting(pid);
				return false;
			}				
			return true;

		}

		/**
		 * A waiting transaction checks to see if it can acquire the read/write
		 * lock and returns true/false if we are still waiting or not.
		 * True = we are still waiting and did not acquire lock
		 * False = we acquired lock and are not still waiting
		 * @param tid
		 * @param pid
		 * @return
		 */
		private synchronized boolean checkReadWrite(TransactionId tid, PageId pid)
		{
			LockRequest req = lockTable.tableEntries.get(pid);
			//if we have the lock then this is the read lock request and we need to check the next one
			if(req.getTransactionId().equals(tid) && holdsLock(tid, pid))
			{
				req = req.next();
			}
			if(req.getTransactionId().equals(tid)) //are we the first request? (so that no others have lock) If not go to sleep
			{
				req.setGranted(true);
				LockNode ln = lockTable.tidLocks.get(tid);
				ln.finishWaiting(pid);
				lockTable.tableEntries.put(pid, req);
				return false;
			}				
			return true;
		}


		/**
		 * This method is only called when we are assigning the lock on pid
		 * to the transaction tid.  It updates lockTable.tidLocks to reflect the
		 * fact that tid has acquired the lock on pid.
		 * @param tid
		 * @param pid
		 */
		private synchronized void createLockNode(TransactionId tid, PageId pid)
		{
			if(lockTable.tidLocks.containsKey(tid))
			{
				LockNode ln = lockTable.tidLocks.get(tid);
				ln.acquireLock(pid);
			}
			else
			{
				LockNode ln = new LockNode();
				ln.acquireLock(pid);
				lockTable.tidLocks.put(tid, ln);
			}	
		}

		/**
		 * This method first checks if we are doing a graph based deadlock
		 * check and if we are, tries to detect the cycle before proceeding.
		 * It then updates lockTable.tidLocks so that the entry corresponding
		 * to tid contains our waiting request for pid with perm.
		 * @param tid
		 * @param pid
		 * @param perm
		 * @throws TransactionAbortedException
		 */
		private synchronized void createWaitingLockNode(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException
		{

			if(GRAPH)
			{
				HashSet<TransactionId> visited = waitingFor(tid, perm, pid);
				HashSet<TransactionId> waiting = new HashSet<TransactionId>();
				detectCycle(tid, visited, waiting, perm);
			}
			if(lockTable.tidLocks.containsKey(tid)) //this tid already has some locks
			{
				LockNode ln = lockTable.tidLocks.get(tid);
				ln.startWaiting(pid);
			}
			else
			{
				LockNode ln = new LockNode();
				ln.startWaiting(pid);
				lockTable.tidLocks.put(tid, ln);
			}
		}

		/**
		 * This method handles all of the deadlock graph based
		 * detection.  Using the waitingFor method, we determine
		 * all of the transactions that our original transactionId tid
		 * is waiting for and follow the path of dependencies until we
		 * either visit all of the Tids (we are done) or find our way
		 * back to our original transactionId where we throw the exception.
		 * @param tid  -- Our original transactionId
		 * @param waitingFor -- Tids that we are waiting for at our node in the graph
		 * 						Originally, this is the set of Transactions that our original
		 * 						transaction is waiting for.
		 * @param visited --Set of all Tids already visited
		 * @param perm -- Used in the waitingFor method
		 * @throws TransactionAbortedException
		 */
		private synchronized void detectCycle(TransactionId tid, HashSet<TransactionId> waitingFor, HashSet<TransactionId> visited, Permissions perm) throws TransactionAbortedException
		{
			//first determine which Transactions this one is waiting for
			//I think that here we should the lockRequest already inserted so we can get all Tids before us
			//HashMap

			Iterator<TransactionId> iter = waitingFor.iterator();
			HashSet<TransactionId> waitingFor1 = new HashSet<TransactionId>();
			while(iter.hasNext())
			{
				TransactionId tid1 = iter.next();
				if(!visited.contains(tid1)) //only need to look at further if we have not already visited
				{
					waitingFor1 = waitingFor(tid1, perm, null);
					if(waitingFor1.contains(tid))
					{
						throw new TransactionAbortedException();
					}
					visited.add(tid1);
					detectCycle(tid, waitingFor1, visited, perm);
				}
			}

		}

		/**
		 * This method returns a HashSet of all of the transactionIds that
		 * tid is waiting for. When we call this about/from the running transaction that is
		 * trying to request a new lock, we have to pass along a pageId as
		 * well because we have not updated lockTable.tidLocks to contain the
		 * new transaction (it is only updated if no deadlock has been detected).
		 * @param tid
		 * @param perm
		 * @param pid
		 * @return
		 */
		private synchronized HashSet<TransactionId> waitingFor(TransactionId tid, Permissions perm, PageId pid)
		{
			HashSet<TransactionId> waitingFor = new HashSet<TransactionId>();
			//find pid that tid is waiting for
			PageId wait;
			if(pid == null)
			{
				LockNode node = lockTable.tidLocks.get(tid);
				wait = node.getWaiting();
			}
			else
			{
				wait = pid;
			}
			LockRequest req = lockTable.tableEntries.get(wait);
			while(req != null && (!req.getTransactionId().equals(tid) || req.granted()))
			{
				//can never be waiting for granted read locks
				if(perm.equals(Permissions.READ_WRITE) || !req.granted() || !req.shared())
				{
					waitingFor.add(req.getTransactionId());
				}
				req = req.next();
			}
			waitingFor.remove(tid);
			return waitingFor;

		}

		/**
		 * Tests if tid already holds the lock on p
		 * @param tid
		 * @param p
		 * @return
		 */
		public synchronized boolean holdsLock(TransactionId tid, PageId p) {
			if(lockTable.tidLocks.containsKey(tid)) 
			{
				LockNode lock = lockTable.tidLocks.get(tid);
				return lock.holdsLock(p);
			}
			return false;
		}

		/**
		 * Prints the list of LockRequests for pid
		 * @param pid
		 */
		public synchronized void printLockRequests(PageId pid)
		{
			LockRequest req = lockTable.tableEntries.get(pid);
			while(req != null)
			{
				req.print();
				req = req.next();
			}
		}

	}
	class LockRequest
	{
		private boolean granted;
		private Permissions perm;
		private TransactionId tid;
		private LockRequest next;

		public LockRequest(TransactionId tid, boolean granted, Permissions perm)
		{
			this.granted = granted;
			this.perm = perm;
			this.tid = tid;
			next = null;
		}

		public TransactionId getTransactionId()
		{
			return tid;
		}

		public boolean granted()
		{
			return granted;
		}

		public boolean shared()
		{
			return (perm.equals(Permissions.READ_ONLY));
		}

		public void setGranted(boolean g)
		{
			granted  = g;
		}

		public void setShared(Permissions p)
		{
			perm = p;
		}

		public LockRequest next()
		{
			return next;
		}

		public void setNext(LockRequest next)
		{
			this.next = next;
		}

		public void print()
		{
			System.out.print("   " + tid + " " + perm + " " + granted);
		}
	}

	class LockNode
	{
		private HashSet<PageId> holding;
		private PageId waiting;

		public LockNode()
		{
			holding = new HashSet<PageId>();
			waiting = null;
		}

		public boolean holdsLock(PageId pid)
		{
			return holding.contains(pid);
		}

		public void acquireLock(PageId pid)
		{
			holding.add(pid);
			waiting = null;
		}

		public void releaseLocks(PageId pid)
		{
			waiting = null;
			holding.remove(pid);
		}

		public void startWaiting(PageId pid)
		{
			waiting = pid;
		}

		public void finishWaiting(PageId pid)
		{
			if(waiting.equals(pid))
			{
				waiting = null;
				holding.add(pid);
			}
			else
			{
				throw new RuntimeException("Not waiting on Lock for this page!");
			}
		}

		public PageId getWaiting()
		{
			return waiting;
		}

		public Iterator<PageId> heldIter()
		{
			return holding.iterator();
		}
	}
}
