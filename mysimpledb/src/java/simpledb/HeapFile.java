package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc tupdes;
	private int tableid;
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
    	if (f == null || td == null)
    	{
    		throw new RuntimeException();
    	}
        file = f;
        tupdes = td;
        tableid = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupdes;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        //calculate page size
    	if (pid == null)
    	{
    		throw new RuntimeException();
    	}
    	int pagesize = BufferPool.getPageSize();
    	InputStream input = null;
    	try {
    		input = new BufferedInputStream(new FileInputStream(file), pagesize);
    		input.skip(pagesize * pid.pageNumber());
    		byte [] data = new byte[pagesize];
    		if (input.read(data, 0, pagesize) == pagesize)
    		{
    			input.close();
    			return new HeapPage((HeapPageId) pid, data);
    		}
    	}
    	catch (FileNotFoundException x) {
    		System.err.print("File Not Found");
    		return null;
    	} 
    	catch (IOException y) {
    		System.err.print("Error reading data");
		}
    	try {
			input.close();
			return null;
		} catch (IOException e) {
			System.err.print("Input Stream Not Closed");
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        int pagenum = ((HeapPageId) page.getId()).pageNumber();
        raf.seek(pagenum * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	int pagesize = BufferPool.getPageSize();
    	int numpages = 0;
    	InputStream input = null;
    	try {
    		input = new BufferedInputStream(new FileInputStream(file), pagesize);
    		numpages = input.available()/pagesize;
    		input.close();
    		return numpages;
    	}
    	catch (FileNotFoundException x) {
    		System.err.print("File Not Found");
    		return -1;
    	} 
    	catch (IOException y) {
    		System.err.print("Error reading data");
		}
    	try {
			input.close();
			return -1;
		} catch (IOException e) {
			System.err.print("Input Stream Not Closed");
			return -1;
		}
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	if (t == null)
    	{
    		System.err.println("Tuple is null, aborting transaction!");
    		throw new TransactionAbortedException();
    	}
        //loop through the pages and look for open slot
    	int i = 0;
    	boolean found = false;
    	HeapPage currpage;
    	ArrayList<Page> pages = new ArrayList<Page>();
    	while (i < numPages())
    	{
    		HeapPageId currentid = new HeapPageId(tableid, i);
    		currpage = (HeapPage) Database.getBufferPool().getPage(tid, currentid, null);
    		if (currpage.getNumEmptySlots() > 0)
    		{
    			currpage.insertTuple(t);
    	        pages.add(currpage);
    			return pages;
    		}
    		i++;
    	}
    	HeapPageId newid = new HeapPageId(tableid, i);
    	HeapPage newpage = new HeapPage(newid, HeapPage.createEmptyPageData());
    	newpage.insertTuple(t);
    	BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
    	bos.write(newpage.getPageData());
    	bos.flush();
    	Database.getBufferPool().getPage(tid, newid, null); //put new page in buffer
    	pages.add(newpage);
    	return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	if (t == null)
    	{
    		System.err.println("Tuple is null, aborting transaction!");
    		throw new TransactionAbortedException();
    	}
        RecordId rec = t.getRecordId();
        PageId delpageid = rec.getPageId();
        HeapPage delpage = (HeapPage) Database.getBufferPool().getPage(tid, delpageid, null);
        delpage.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<Page>();
        pages.add(delpage);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        
    	return new Myiterator(tid);
    }

	class Myiterator implements DbFileIterator {
		private boolean open = false;
		private int index;
		private HeapPage currpage;
		private TransactionId transid;
		private Iterator iterup;
		private boolean nextfound = false;
		private Tuple nexttup;
		private int numpages;
		
		public Myiterator(TransactionId tid)
		{
			transid = tid;
			index = 0;
			numpages = numPages();
		}
		
		public void open() throws DbException, TransactionAbortedException
		{
			//need to check if this is in the table?
			if (!open)
			{
				index = 0;
				HeapPageId currentid = new HeapPageId(tableid, index);
				currpage = (HeapPage) Database.getBufferPool().getPage(transid, currentid, null);
				iterup = currpage.iterator();
				open = true;
			}
			else
			{
				System.err.println("Iterator already open!");
			}
		}

	    /**
	     * @return true if there are more tuples available.
	     */
	    public boolean hasNext() throws DbException, TransactionAbortedException {
	    	if (open)
	    	{
	    		if (nextfound == true)
	    		{
	    			return true;
	    		}
	    		if (iterup.hasNext() == true)
	    		{
	    			nexttup = (Tuple) iterup.next();
	    			nextfound = true;
	    			return true;
	    		}
	    		else
	    		{
	    			while (true)
	    			{
	    				if (index == numpages) // already know at last page
	    				{
	    					return false;
	    				}
	    				index++;
	    				if (index == numpages) // moving to last page?
	    				{
	    					return false;
	    				}
	    				HeapPageId currentid = new HeapPageId(tableid, index);
	    				currpage = (HeapPage) Database.getBufferPool().getPage(transid, currentid, null);
	    				iterup = currpage.iterator();
	    				if (iterup.hasNext() == true)
	    				{
	    					nexttup = (Tuple) iterup.next();
	    					nextfound = true;
	    					return true;
	    				}
	    			}
	    		}
	    	}
	    	else
	    	{
	    		return false;
	    	}
	    }

	    /**
	     * Gets the next tuple from the operator (typically implementing by reading
	     * from a child operator or an access method).
	     *
	     * @return The next tuple in the iterator.
	     * @throws NoSuchElementException if there are no more tuples
	     */
	    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
	    {
	    	if (!open || !hasNext()) 
	    	{
	    		throw new NoSuchElementException("");
	    	}
	    	else
	    	{
	    		nextfound = false;
	    		return nexttup;
	    	}
	    }

	    /**
	     * Resets the iterator to the start.
	     *
	     * @throws DbException When rewind is unsupported.
	     */
	    public void rewind() throws DbException, TransactionAbortedException
	    {
	    	index = 0;
	    	nextfound = false;
	    	open = false;
	    	open();
		
	    }

	    /**
	     * Closes the iterator.
	     */
	    public void close()
	    {
	    	open = false;
	    	nextfound = false;
	    	
	    }
	}
    
}

