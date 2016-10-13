package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;

    private TupleDesc td;

    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.numPages = (int)(this.file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.pageno() < 0 || pid.pageno() >= this.numPages) {
            throw new IllegalArgumentException();
        }
        Page page = null;
        try {
            FileInputStream fis = new FileInputStream(this.file);
            fis.skip(BufferPool.PAGE_SIZE * pid.pageno());
            byte[] data = HeapPage.createEmptyPageData();
            if (fis.read(data) == BufferPool.PAGE_SIZE) {
                page = new HeapPage(new HeapPageId(this.getId(), pid.pageno()), data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        FileOutputStream fos = new FileOutputStream(this.file);
        FileChannel ch = fos.getChannel();
        ch.position(BufferPool.PAGE_SIZE * page.getId().pageno());
        ch.write(ByteBuffer.wrap(page.getPageData()));
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Page modifiedPage = null;
        for (int i = 0; i <= this.numPages; ++i) {
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), null);
            if (page.getNumEmptySlots() > 0) {
                page.addTuple(t);
                modifiedPage = page;
                if (i == this.numPages) {
                    ++this.numPages;
                }
                break;
            }
        }
        ArrayList<Page> result = new ArrayList<Page>();
        result.add(modifiedPage);
        return result;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        HeapPage modifiedPage = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        modifiedPage.deleteTuple(t);
        return modifiedPage;
        // not necesssor lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileItr();
    }

    private class HeapFileItr implements DbFileIterator {

        private int pageno;

        private Iterator<Tuple> tupleItr;

        private void findNext() throws DbException, TransactionAbortedException {
            PageId id;
            while (this.tupleItr != null && !this.tupleItr.hasNext()) {
                ++this.pageno;
                id = new HeapPageId(getId(), this.pageno);
                this.tupleItr = numPages == this.pageno ? null:
                        ((HeapPage) Database.getBufferPool().getPage(null, id, null)).iterator();
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.rewind();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return tupleItr != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleItr == null) {
                throw new NoSuchElementException();
            }
            Tuple res = this.tupleItr.next();
            this.findNext();
            return res;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageno = 0;
            PageId id = new HeapPageId(getId(), 0);
            this.tupleItr = numPages == 0 ? null :
                    ((HeapPage) Database.getBufferPool().getPage(null, id, null)).iterator();
            this.findNext();
        }

        @Override
        public void close() {
            tupleItr = null;
        }
    }
    
}

