package simpledb;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int estScanCost;

    private int numTuples;

    private IntHistogram[] intHists;

    private StringHistogram[] stringHists;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        TransactionId tid = new TransactionId();
        BufferPool bufferPool = Database.getBufferPool();
        boolean finished = false;
        while (!finished) {
            try {
                try {
                    this.numTuples = 0;
                    Integer[] minInts = new Integer[tupleDesc.numFields()];
                    Integer[] maxInts = new Integer[tupleDesc.numFields()];
                    DbFileIterator iter = dbFile.iterator(tid);
                    iter.open();
                    while (iter.hasNext()) {
                        Tuple tuple = iter.next();
                        for (int i = 0; i != tupleDesc.numFields(); ++i) {
                            switch (tupleDesc.getType(i)) {
                                case INT_TYPE:
                                    Integer intVal = ((IntField) tuple.getField(i)).getValue();
                                    minInts[i] = minInts[i] == null || minInts[i] > intVal ? intVal : minInts[i];
                                    maxInts[i] = maxInts[i] == null || maxInts[i] < intVal ? intVal : maxInts[i];
                                    break;
                            }
                        }
                        ++this.numTuples;
                    }
                    this.estScanCost = ((HeapFile)dbFile).numPages() * ioCostPerPage;
                    this.intHists = new IntHistogram[tupleDesc.numFields()];
                    this.stringHists = new StringHistogram[tupleDesc.numFields()];
                    iter.rewind();
                    for (int i = 0; i != tupleDesc.numFields(); ++i) {
                        switch (tupleDesc.getType(i)) {
                            case INT_TYPE:
                                this.intHists[i] = new IntHistogram(NUM_HIST_BINS, minInts[i], maxInts[i]);
                                break;
                            case STRING_TYPE:
                                this.stringHists[i] = new StringHistogram(NUM_HIST_BINS);
                                break;
                        }
                    }
                    while (iter.hasNext()) {
                        Tuple tuple = iter.next();
                        for (int i = 0; i != tupleDesc.numFields(); ++i) {
                            switch (tupleDesc.getType(i)) {
                                case INT_TYPE:
                                    this.intHists[i].addValue(((IntField)tuple.getField(i)).getValue());
                                    break;
                                case STRING_TYPE:
                                    this.stringHists[i].addValue(((StringField)tuple.getField(i)).getValue());
                                    break;
                            }
                        }
                    }
                    iter.close();
                    finished = true;
                    bufferPool.transactionComplete(tid);
                } catch (TransactionAbortedException e) {
                    bufferPool.transactionComplete(tid, false);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
        return this.estScanCost;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
        return (int)(this.numTuples * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	// some code goes here
        double est = 0;
        switch (constant.getType()) {
            case INT_TYPE:
                est = intHists[field].estimateSelectivity(op, ((IntField)constant).getValue());
                break;
            case STRING_TYPE:
                est = stringHists[field].estimateSelectivity(op, ((StringField)constant).getValue());
                break;
        }
        return est;
    }

}
