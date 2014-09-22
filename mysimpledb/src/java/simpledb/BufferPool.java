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
		// some code goes here
		// not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
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
		// some code goes here
		// not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
		return false;
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
		for (int i = 0; i < pages.size(); i ++)
		{
			Page dirpage = pages.get(i);
			dirpage.markDirty(true, tid);
			PageId dirid = dirpage.getId();
			idtopage.put(dirid, dirpage);
			idtotime.put(dirid, new Long(System.currentTimeMillis()));
		}
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
		for (int i = 0; i < pages.size(); i ++)
		{
			Page dirpage = pages.get(i);
			dirpage.markDirty(true, tid);
			PageId dirid = dirpage.getId();
			idtopage.put(dirid, dirpage);
			idtotime.put(dirid, new Long(System.currentTimeMillis()));
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
		Iterator<PageId> iter = (idtotime.keySet()).iterator();
		if (pagespresent > 0)
		{
			PageId key = iter.next();
			PageId minkey = key;
			long mintime = idtotime.get(key);
			long temptime = idtotime.get(key);
			while (iter.hasNext())
			{
				key = iter.next();
				temptime = idtotime.get(key);
				if (temptime < mintime)
				{
					mintime = temptime;
					minkey = key;
				}
			}
			try {
				flushPage(minkey);
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			idtotime.remove(minkey);
			idtopage.remove(minkey);
			pagespresent--;
		}
		//find least recently used disk
		//flush page to disk
		//remove from thing
		//decresae pagecount
		// not necessary for lab1
	}

}
