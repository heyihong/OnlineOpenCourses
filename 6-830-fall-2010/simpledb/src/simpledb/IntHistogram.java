package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;

    private int min;

    private int max;

    private int bsize;

    private int border;

    private int total;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.bsize = (max - min + 1) / buckets;
        this.border = buckets - (max - min + 1) % buckets;
        this.total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (this.min <= v && v <= this.max) {
            ++this.buckets[this.bucketid(v)];
            ++this.total;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        if (v < this.min) {
            return op.equals(Predicate.Op.NOT_EQUALS) ||
                    op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ) ? 1 : 0;
        }
        if (v > this.max) {
            return op.equals(Predicate.Op.NOT_EQUALS) ||
                    op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ) ? 1 : 0;
        }
        int bid = this.bucketid(v);
        int l = bid * this.bsize + this.min + Math.max(0, bid - this.border);
        int r = l + this.bsize + (bid < this.border ? 0 : 1) - 1;
        double est = 0.0;
        switch (op) {
            case EQUALS:
                est = (double) this.buckets[bid] / (r - l + 1);
                break;
            case NOT_EQUALS:
                est = total - (double)this.buckets[bid] / (r - l + 1);
                break;
            case GREATER_THAN:case GREATER_THAN_OR_EQ:
                est = (double) this.buckets[bid] * (r - v + (op.equals(Predicate.Op.GREATER_THAN_OR_EQ) ? 1 : 0)) / (r - l + 1);
                for (++bid; bid < this.buckets.length; ++bid) {
                    est += this.buckets[bid];
                }
                break;
            case LESS_THAN:case LESS_THAN_OR_EQ:
                est= (double)this.buckets[bid] * (v - l + (op.equals(Predicate.Op.LESS_THAN_OR_EQ) ? 1 : 0)) / (r - l + 1);
                for (--bid; bid >= 0; --bid) {
                    est += this.buckets[bid];
                }
                break;
        }
        return est / this.total;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }

    private int bucketid(int v) {
        return v - min < this.bsize * this.border
                ? (v - min) / this.bsize : (v - min - this.bsize * this.border) / (this.bsize + 1) + this.border;
    }
}
