package simpledb;

import java.io.*;

import java.util.ArrayList;
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
	public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		if(idtopage.containsKey(pid))
		{
			idtotime.put(pid, new Long(System.currentTimeMillis()));
			acquireLock(tid, pid, perm);
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
			acquireLock(tid, pid, perm);
			return newpage;
		}
	}

	private void acquireLock(TransactionId tid, PageId pid, Permissions perm)
	{
		boolean waiting = true;
		boolean noexclusive = true;
		boolean allgranted = true;
		//does this transaction already hold the lock?
		if(holdsLock(tid, pid))
		{
			if(perm.equals(Permissions.READ_ONLY))
			{
				waiting = false;
			}
			else //read/write
			{
				synchronized(lockTable)
				{
					LockRequest req = lockTable.tableEntries.get(pid);
					if(req.getTransactionId().equals(tid)) //if not first one then upgrade
					{
						if(req.shared())  //have read only lock, need to upgrade
						{
							if(req.next() != null && req.next().granted()) //next one is granted
							{
								lockTable.tableEntries.put(pid, req.next()); //remove first entry
								req = req.next();
								while (req.next() != null && req.next().granted)
								{
									req = req.next();
								}
								//req.next is not granted we want to insert here
								LockRequest newreq = new LockRequest(tid, true, perm);
								newreq.setNext(req.next());
								req.setNext(newreq);
							}
						}
						else //already have read/write lock
						{
							waiting = false;
						}
					}
					else
					{ //can't have read/write lock here
						while(!req.next().getTransactionId().equals(tid)) //find this request
						{
							req = req.next();
						}
						req.setNext(req.next().next());//remove current request
						while(req.next() != null && req.next().granted() != false) //find spot before requests without lock
						{
							req = req.next();
						}
						LockRequest newreq = new LockRequest(tid, true, perm);
						newreq.setNext(req.next());
						req.setNext(newreq);
					}
				}
			}
			//insert after all read only access in queue
		}
		else //transaction doesn't already hold lock
		{
			//check if no locks held on page
			synchronized(lockTable)
			{
				if(!lockTable.tableEntries.containsKey(pid))
				{
					lockTable.tableEntries.put(pid, new LockRequest(tid, true, perm));
					if(lockTable.heldLocks.containsKey(tid))
					{
						LockNode ln = lockTable.heldLocks.get(tid);
						while(ln.next() != null)
						{
							ln = ln.next();
						}
						ln.setNext(new LockNode(pid, perm));
					}
					else
					{
						lockTable.heldLocks.put(tid, new LockNode(pid, perm));
					}
					waiting = false;
				}
				//if locks exist, add new entries
				else
				{
					LockRequest lr = lockTable.tableEntries.get(pid);
					if(!lr.shared())
					{
						noexclusive = false;
					}
					if(!lr.granted())
					{
						allgranted = false;
					}
					while(lr.next() != null)
					{
						if(!lr.shared())
						{
							noexclusive = false;
						}
						if(!lr.granted())
						{
							allgranted = false;
						}
						lr = lr.next();
					}
					lr.setNext(new LockRequest(tid, false, perm));
					if (allgranted && noexclusive && perm.equals(Permissions.READ_ONLY))
					{
						lr.next().setGranted(true);
						if(lockTable.heldLocks.containsKey(tid))
						{
							LockNode ln = lockTable.heldLocks.get(tid);
							while(ln.next() != null)
							{
								ln = ln.next();
							}
							ln.setNext(new LockNode(pid, perm));
						}
						else
						{
							lockTable.heldLocks.put(tid, new LockNode(pid, perm));
						}
						waiting = false;
					}	
				}
			}
		}

		//now lets start waiting for the lock

		if(perm.equals(Permissions.READ_ONLY))
		{
			//if read only then we need to make sure that every request before this one
			//is also a shared lock and has been granted
			while (waiting)
			{
				allgranted = true;
				noexclusive = true;
				LockRequest req = lockTable.tableEntries.get(pid);
				synchronized (lockTable) 
				{
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
					if (allgranted && noexclusive)
					{
						req.setGranted(true);
						if(lockTable.heldLocks.containsKey(tid))
						{
							LockNode ln = lockTable.heldLocks.get(tid);
							while(ln.next() != null)
							{
								ln = ln.next();
							}
							ln.setNext(new LockNode(pid, perm));
						}
						else
						{
							lockTable.heldLocks.put(tid, new LockNode(pid, perm));
						}
						waiting = false;
					}				
				}
				if (waiting) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException ignored) { }
				}
			}
		}
		else
		{
			//if exclusive lock, then we need to make sure that there are no other requests before this one
			while (waiting) 
			{	
				synchronized (lockTable) 
				{
					LockRequest req = lockTable.tableEntries.get(pid);
					if(req.getTransactionId().equals(tid))
					{
						req.setGranted(true);
						if(lockTable.heldLocks.containsKey(tid))
						{
							LockNode ln = lockTable.heldLocks.get(tid);
							while(ln.next() != null)
							{
								ln = ln.next();
							}
							ln.setNext(new LockNode(pid, perm));
						}
						else
						{
							lockTable.heldLocks.put(tid, new LockNode(pid, perm));
						}
						waiting = false;
					}				
				}
				if (waiting) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException ignored) { }
				}
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
			LockNode ln = lockTable.heldLocks.get(tid);
			if(ln.getPageId().equals(pid)) //first entry
			{
				if(ln.next() == null)
				{
					lockTable.heldLocks.remove(tid);
				}
				else
				{
					lockTable.heldLocks.put(tid, ln.next());
				}
			}
			else
			{
				while(!ln.next().getPageId().equals(pid))
				{
					ln = ln.next();
				}
				ln.setNext(ln.next().next());
			}

			// update lockEntries
			LockRequest lr = lockTable.tableEntries.get(pid);
			if(lr.getTransactionId().equals(tid)) //first entry
			{
				if(lr.next() == null)
				{
					lockTable.tableEntries.remove(pid);
				}
				else
				{
					lockTable.tableEntries.put(pid, lr.next());
				}
			}
			else
			{
				while(!lr.next().getTransactionId().equals(tid))
				{
					lr = lr.next();
				}
				lr.setNext(lr.next().next());
			}

		}
	}


	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		synchronized (lockTable) {
			if(lockTable.heldLocks.containsKey(tid)) //
			{
				LockNode lock = lockTable.heldLocks.get(tid);
				while (lock != null)
				{
					if(lock.getPageId().equals(p)) //only added to heldLocks once the Transaction has the lock
					{
						return true;
					}
					lock = lock.next();
				}
			}
			return false;
		}
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
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
				idtopage.put(dirid, dirpage);
				idtotime.put(dirid, new Long(System.currentTimeMillis()));
				numpages++;
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
		// some code goes here
		// only necessary for lab6                                                                            // cosc460
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
			if(flpage.isDirty() != null)
			{
				dbdel.writePage(flpage);
				flpage.markDirty(false, new TransactionId());
			}
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
	}

	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		synchronized(this)
		{
			Iterator<PageId> iter = (idtotime.keySet()).iterator();
			if (pagespresent > 0)
			{
				PageId key = iter.next();
				PageId minkey = key;
				long mintime = Long.MAX_VALUE;
				long temptime = idtotime.get(key);
				if (temptime < mintime && idtopage.get(key).isDirty() == null)
				{
					mintime = temptime;
					minkey = key;
				}
				while (iter.hasNext())
				{
					key = iter.next();
					temptime = idtotime.get(key);
					if (temptime < mintime && idtopage.get(key).isDirty() == null)
					{
						mintime = temptime;
						minkey = key;
					}
				}
				if (mintime == Long.MAX_VALUE)
				{
					throw new DbException("All pages dirty!");
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
		//find least recently used disk
		//flush page to disk
		//remove from thing
		//decresae pagecount
		// not necessary for lab1
	}

	class LockTable
	{
		ConcurrentHashMap<PageId, LockRequest> tableEntries;
		ConcurrentHashMap<TransactionId, LockNode> heldLocks;

		public LockTable()
		{
			tableEntries = new ConcurrentHashMap<PageId, LockRequest>();
			heldLocks = new ConcurrentHashMap<TransactionId, LockNode>();
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
	}

	class LockNode
	{
		private PageId pid;
		private LockNode next;
		private Permissions perm;

		public LockNode(PageId pid, Permissions perm)
		{
			this.pid = pid;
			next = null;
			this.perm = perm;
		}

		public PageId getPageId()
		{
			return pid;
		}

		public LockNode next()
		{
			return next;
		}

		public void setNext(LockNode next)
		{
			this.next = next;
		}

		public Permissions getPermission()
		{
			return perm;
		}
	}
}
