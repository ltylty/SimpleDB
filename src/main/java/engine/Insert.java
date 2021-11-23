package engine;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    TransactionId t;
    DbIterator child;
    int tableid;

    TupleDesc td;
    Tuple insertResult;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	
    	if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
            throw new DbException("The TupleDesc of child differs from table into which we are to insert");
        }
    	
    	this.t = t;
    	this.child = child;
    	this.tableid = tableid;
    	
    	td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Insert"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return  td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		// return null;

		if (insertResult != null)
			return null;

		int numTup = 0;

		while (child.hasNext()) {
			try {
				Database.getBufferPool().insertTuple(t, tableid, child.next());
				numTup++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		insertResult = new Tuple(getTupleDesc());
		Field affectf = new IntField(numTup);
		insertResult.setField(0, affectf);

		return insertResult;

	}

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	children[0] = child;
    }
}
