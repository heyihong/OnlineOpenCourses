package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    private TransactionId tid;
    private DbIterator child;
    private Integer numRecords;
    private TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        Type[] types = new Type[1];
        String[] fields = new String[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types, fields);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        this.numRecords = 0;
    }

    public void close() {
        // some code goes here
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.numRecords = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple = null;
        if (this.numRecords != null) {
            BufferPool bufferPool = Database.getBufferPool();
            try {
                Tuple t;
                while (this.child.hasNext()) {
                    t = this.child.next();
                    bufferPool.deleteTuple(tid, t);
                    ++this.numRecords;
                }
            } catch (TransactionAbortedException e) {
                throw e;
            } catch (Exception e) {
                throw new DbException(e.getMessage());
            }
            tuple = new Tuple(this.tupleDesc);
            tuple.setField(0, new IntField(this.numRecords));
            this.numRecords = null;
        }
        return tuple;
    }
}
