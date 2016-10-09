package simpledb;
import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private TransactionId tid;

    private int tableid;

    private String tableAlias;

    private DbFileIterator dbFileItr;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.dbFileItr = Database.getCatalog().getDbFile(this.tableid).iterator(tid);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        // some code goes here
        this.dbFileItr.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return Database.getCatalog().getTupleDesc(this.tableid);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return dbFileItr.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        return dbFileItr.next();
    }

    public void close() {
        // some code goes here
        dbFileItr.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        dbFileItr.rewind();
    }
}
