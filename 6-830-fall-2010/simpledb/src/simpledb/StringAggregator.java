package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> fieldToCount;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.fieldToCount = new HashMap<Field, Integer>();
        Type[] typeAr;
        String[] fieldAr;
        int numFields = gbfield == -1 ? 1 : 2;
        typeAr = new Type[numFields];
        fieldAr = new String[numFields];
        if (numFields == 2) {
            typeAr[0] = this.gbfieldtype;
            fieldAr[0] = null;
        }
        typeAr[numFields - 1] = Type.INT_TYPE;
        fieldAr[numFields - 1] = Aggregate.aggName(what);
        this.tupleDesc = new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
        Field field = this.gbfield == -1 ? null : tup.getField(this.gbfield);
        this.fieldToCount.put(field, this.fieldToCount.getOrDefault(field, 0) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new StringAggregatorItr();
    }

    private class StringAggregatorItr implements DbIterator {

        private Iterator<Map.Entry<Field, Integer>> iter;

        public StringAggregatorItr() {
            this.iter = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iter = fieldToCount.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return this.iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(tupleDesc);
            Map.Entry<Field, Integer> entry = this.iter.next();
            IntField intField = new IntField(entry.getValue());
            if (gbfield == -1) {
                tuple.setField(0, intField);
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, intField);
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.iter = fieldToCount.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            this.iter = null;
        }
    }
}
