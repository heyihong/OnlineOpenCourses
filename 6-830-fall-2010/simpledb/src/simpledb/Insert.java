package simpledb;
import javax.xml.crypto.Data;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    private TransactionId tid;
    private DbIterator child;
    private Integer numRecords;
    private int tableid;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
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
        throw new UnsupportedOperationException("Cannot rewind");
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple = null;
        if (this.numRecords != null) {
            BufferPool bufferPool = Database.getBufferPool();
            try {
                while (this.child.hasNext()) {
                    bufferPool.insertTuple(this.tid, this.tableid, this.child.next());
                    ++this.numRecords;
                }
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
