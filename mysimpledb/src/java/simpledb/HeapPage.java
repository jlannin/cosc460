package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

	final HeapPageId pid;
	final TupleDesc td;
	final byte header[];
	final Tuple tuples[];
	final int numSlots;
	private boolean dirty = false;
	private boolean logDirty = false;
	private TransactionId dirtTrans = null;
	

	byte[] oldData;
	private final Byte oldDataLock = new Byte((byte) 0);

	/**
	 * Create a HeapPage from a set of bytes of data read from disk.
	 * The format of a HeapPage is a set of header bytes indicating
	 * the slots of the page that are in use, some number of tuple slots.
	 * Specifically, the number of tuples is equal to: <p>
	 * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
	 * <p> where tuple size is the size of tuples in this
	 * database table, which can be determined via {@link Catalog#getTupleDesc}.
	 * The number of 8-bit header words is equal to:
	 * <p/>
	 * ceiling(no. tuple slots / 8)
	 * <p/>
	 *
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 */
	public HeapPage(HeapPageId id, byte[] data) throws IOException {
		this.pid = id;
		this.td = Database.getCatalog().getTupleDesc(id.getTableId());
		this.numSlots = getNumTuples();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i = 0; i < header.length; i++)
			header[i] = dis.readByte();

		tuples = new Tuple[numSlots];
		try {
			// allocate and read the actual records of this page
			for (int i = 0; i < tuples.length; i++)
				tuples[i] = readNextTuple(dis, i);
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		}
		dis.close();

		setBeforeImage();
	}

	/**
	 * Retrieve the number of tuples on this page.
	 *
	 * @return the number of tuples on this page
	 */
	private int getNumTuples() {
		int tuplesize = (Database.getCatalog().getTupleDesc(pid.getTableId())).getSize();
		int numtuples = (int) Math.floor(BufferPool.getPageSize()*8/(tuplesize*8 + 1));
		return numtuples;

	}

	/**
	 * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
	 *
	 * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
	 */
	private int getHeaderSize() {
		return (int) Math.ceil(getNumTuples()/8.0);
	}

	/**
	 * Return a view of this page before it was modified
	 * -- used by recovery
	 */
	public HeapPage getBeforeImage() {
		try {
			byte[] oldDataRef = null;
			synchronized (oldDataLock) {
				oldDataRef = oldData;
			}
			return new HeapPage(pid, oldDataRef);
		} catch (IOException e) {
			e.printStackTrace();
			//should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized (oldDataLock) {
			oldData = getPageData().clone();
		}
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public HeapPageId getId() {
		return pid;
	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!isSlotUsed(slotId)) {
			for (int i = 0; i < td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		RecordId rid = new RecordId(pid, slotId);
		t.setRecordId(rid);
		try {
			for (int j = 0; j < td.numFields(); j++) {
				Field f = td.getFieldType(j).parse(dis);
				t.setField(j, f);
			}
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 * <p/>
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the HeapPage constructor and
	 * have it produce an identical HeapPage object.
	 *
	 * @return A byte array correspond to the bytes of this page.
	 * @see #HeapPage
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i = 0; i < header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i = 0; i < tuples.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j = 0; j < td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j = 0; j < td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					f.serialize(dos);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Static method to generate a byte array corresponding to an empty
	 * HeapPage.
	 * Used to add new, empty pages to the file. Passing the results of
	 * this method to the HeapPage constructor will create a HeapPage with
	 * no valid tuples in it.
	 *
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		int len = BufferPool.getPageSize();
		return new byte[len]; //all 0
	}

	/**
	 * Delete the specified tuple from the page;  the tuple should be updated to reflect
	 * that it is no longer stored on any page.
	 *
	 * @param t The tuple to delete
	 * @throws DbException if this tuple is not on this page, or tuple slot is
	 *                     already empty.
	 */
	public void deleteTuple(Tuple t) throws DbException {
		if(numSlots == getNumEmptySlots())
		{
			throw new DbException("No Tuples on page!");
		}
		if (t == null)
		{
			throw new DbException("Tuple was null!");
		}
		RecordId rid = t.getRecordId();
		if (rid == null)
		{
			throw new DbException("Tuple already deleted!");
		}
		int tuplenum = rid.tupleno();
		PageId tpid = rid.getPageId();
		if(!tpid.equals(pid))
		{
			throw new DbException("Tuple not on page!");
		}
		if(isSlotUsed(tuplenum) == false)
		{
			throw new DbException("Tuple slot already empty!");
		}
		tuples[tuplenum].setRecordId(null);
		markSlotUsed(tuplenum, false);
		
	}

	/**
	 * Adds the specified tuple to the page;  the tuple should be updated to reflect
	 * that it is now stored on this page.
	 *
	 * @param t The tuple to add.
	 * @throws DbException if the page is full (no empty slots) or tupledesc
	 *                     is mismatch.
	 */
	public void insertTuple(Tuple t) throws DbException {
		if (t == null)
		{
			throw new DbException("Tuple was null!");
		}
		if (!t.getTupleDesc().equals(td))
		{
			throw new DbException("Tuple Description of page doesn't match that of new tuple to insert!");
		}
		if (getNumEmptySlots()== 0)
		{
			throw new DbException("No empty slots!");
		}
		int i = 0;
		boolean looking = true;
		while (looking)
		{
			if(isSlotUsed(i) == false) //found empty slot
			{
				RecordId newrec = new RecordId(pid, i);
				t.setRecordId(newrec);
				tuples[i] = t;
				markSlotUsed(i, true);
				looking = false;
			}
			i++;
		}
		
	}

	/**
	 * Marks this page as dirty/not dirty and record that transaction
	 * that did the dirtying
	 */
	public void markDirty(boolean dirty, TransactionId tid) {
		this.dirty = dirty;
		dirtTrans = tid;
		if(dirty) //if we have marked a page dirty, then we need to make sure that we update log before flush
		{
			markLogDirty(true);
		}
	}

	/**
	 * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
	 */
	public TransactionId isDirty() {
		if (dirty)
		{
			return dirtTrans;
		}
		else
		{
		return null;      
		}
	}

	public void markLogDirty(boolean dirty)
	{
		logDirty = dirty;
	}
	
	public boolean logDirty()
	{
		return logDirty;
	}
	
	/**
	 * Returns the number of empty slots on this page.
	 */
	public int getNumEmptySlots() {
		int emptyspaces = 0;
		for (int i = 0; i < header.length-1; i++)
		{
			int curhead = (int) header[i];
			for (int x = 0; x < 8; x++)
			{
				if (curhead%2 == 0)
				{
					emptyspaces++;
				}
				curhead=curhead>>1;
			}
		}
		int remainder = numSlots%8;
		if (remainder == 0 && numSlots != 0)
		{
			remainder = 8;
		}
		int lastheader = (int) header[header.length-1];
		for (int i = 0; i < remainder; i++)
		{
			if (lastheader%2 == 0)
			{
				emptyspaces++;
			}
			lastheader=lastheader>>1;
		}
		return emptyspaces;
	}

	/**
	 * Returns true if associated slot on this page is filled.
	 */
	public boolean isSlotUsed(int i) {
		int headeri = i/8;
		if (headeri >= header.length)
		{
			return false;
		}
		int withinheader = i%8;
		int mask = 1;
		mask = mask<<(withinheader);
		if((header[headeri]&mask) == mask)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * Abstraction to fill or clear a slot on this page.
	 */
	private void markSlotUsed(int i, boolean value) {
		int headeri = i/8;
		if (headeri >= header.length)
		{
			throw new RuntimeException("Index out of header range!");
		}
		int withinheader = i%8;
		int setter = (int) Math.pow(2.0, withinheader);
		if (value)
		{
			header[headeri] += setter;
		}
		else
		{
			header[headeri] -= setter;
		}
	}

	/**
	 * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
	 * (note that this iterator shouldn't return tuples in empty slots!)
	 */
	public Iterator<Tuple> iterator() {

		return new tupleiterator();

	}


	class tupleiterator implements Iterator<Tuple> {

		private int index;
		private boolean nextFound;
		
		public tupleiterator() {
			index = 0;
			nextFound = false;
		}
		
		@Override
		public boolean hasNext() {
			if (nextFound == true)
			{
				return true;
			}
			for (int i = index; i < numSlots; i++)
			{
				if(isSlotUsed(i) == true)
				{
					nextFound = true;
					index = i;
					return true;
				}
			}
			return false;
		}
		
		@Override
		public Tuple next() {
			if (!hasNext()) 
			{
                throw new NoSuchElementException("");
            }
			else
			{
				nextFound = false;
				index++;
				return tuples[(index-1)];
			}
			
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException("No removing data!");
		}

	}
}



