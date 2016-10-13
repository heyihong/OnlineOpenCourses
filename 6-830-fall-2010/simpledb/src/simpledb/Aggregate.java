package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private DbIterator aggItr;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        Type[] typeAr;
        String[] fieldAr;
        int numFields = gfield == -1 ? 1 : 2;
        typeAr = new Type[numFields];
        fieldAr = new String[numFields];
        TupleDesc tupleDesc = child.getTupleDesc();
        if (numFields == 2) {
            typeAr[0] = tupleDesc.getType(gfield);
            fieldAr[0] = tupleDesc.getFieldName(gfield);
        }
        typeAr[numFields - 1] = Type.INT_TYPE;
        fieldAr[numFields - 1] = Aggregate.aggName(aop) + "(" + tupleDesc.getFieldName(afield) + ")";
        this.tupleDesc = new TupleDesc(typeAr, fieldAr);
        switch (tupleDesc.getType(afield)) {
            case INT_TYPE:
                this.aggregator = new IntAggregator(gfield, gfield == -1 ? null : tupleDesc.getType(gfield), afield, aop);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(gfield, gfield == -1 ? null : tupleDesc.getType(gfield), afield, aop);
                break;
        }
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.merge(this.child.next());
        }
        this.child.close();
        this.aggItr = this.aggregator.iterator();
        this.aggItr.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.aggItr.hasNext() ? this.aggItr.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggItr.rewind();

    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void close() {
        // some code goes here
        this.aggItr.close();
    }
}
