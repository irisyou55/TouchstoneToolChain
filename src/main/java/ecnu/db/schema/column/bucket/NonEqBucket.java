package ecnu.db.schema.column.bucket;

import java.math.BigDecimal;

/**
 * @author alan
 */
public class NonEqBucket {
    public BigDecimal probability;
    public String value;
    public NonEqBucket leftBucket;
    public NonEqBucket rightBucket;
    public NonEqBucket search(BigDecimal probability) {
        if (this.probability == null) {
            return this;
        }
        if (probability.compareTo(this.probability) < 0) {
            return leftBucket.search(probability);
        }
        else if (probability.compareTo(this.probability) > 0) {
            return rightBucket.search(probability);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
