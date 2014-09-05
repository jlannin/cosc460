package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private Field[] tup;
    private RecordId recid = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	if (td == null)
    	{
    		throw new RuntimeException();
    	}
        this.td = td;
        tup = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
       return recid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
       if (i < tup.length && i >= 0 && (td.getFieldType(i)).equals(f.getType()))
       {
    	   tup[i] = f;
       }
       else
       {
    	   throw new RuntimeException();
       }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
    	if (i < tup.length && i >= 0)
    	{
    		return tup[i];
    	}
    	else
    	{
    		throw new RuntimeException();
    	}
        
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
       
    	String str =  "";
    	for (int i = 0; i < tup.length; i++)
    	{
    		str += tup[i];
    		if(i != (tup.length-1))
    		{
    			str += " ";
    		}
    	}
    	return str;
        //throw new UnsupportedOperationException("Implement this");
    }

}
