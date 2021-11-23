package engine;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId t;
    DbIterator child;
    
    TupleDesc td;
    Tuple resultTuple;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{"Delete"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(resultTuple != null) {
    		return null;
    	}
    	int deleteNum = 0;
    	while(child.hasNext()) {
    		try {
    			Database.getBufferPool().deleteTuple(t, child.next());
        		deleteNum++;	
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
    	resultTuple = new Tuple(td);
    	resultTuple.setField(0, new IntField(deleteNum));
        return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	children[0] = child;
    }

}
