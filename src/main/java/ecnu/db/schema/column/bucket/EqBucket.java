package ecnu.db.schema.column.bucket;

import java.math.BigDecimal;

/**
 * @author alan
 */
public class EqBucket implements Comparable<EqBucket> {
    public NonEqBucket parent;
    public BigDecimal capacity;
    public BigDecimal leftBorder;
    public BigDecimal rightBorder;

    @Override
    public int compareTo(EqBucket bucket) {
        return bucket.capacity.compareTo(capacity);
    }
}
