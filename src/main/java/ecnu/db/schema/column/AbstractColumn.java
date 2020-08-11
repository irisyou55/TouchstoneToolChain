package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.math.BigDecimal;


class NonEqBucket {
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
            return leftBucket;
        }
    }
}

class EqBucket {
    public NonEqBucket rangeBucket;
    public int capacity;
}

/**
 * @author qingshuai.wang
 */
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;
    protected NonEqBucket bucket;

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        bucket = new NonEqBucket();
        bucket.probability = BigDecimal.ONE;
    }

    /**
     * 获取该列非重复值的个数
     *
     * @return 非重复值的个数
     */
    public abstract int getNdv();

    @JsonGetter
    @SuppressWarnings("unused")
    public float getNullPercentage() {
        return nullPercentage;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    @JsonSetter
    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public void adjustNonEqProbabilityBucket(BigDecimal probability, String value) {
        NonEqBucket bucket = this.bucket.search(probability);
        bucket.value = value;
        bucket.probability = probability;
        bucket.leftBucket = new NonEqBucket();
        bucket.rightBucket = new NonEqBucket();
    }

    public void initEqProbabilityBucket() {

    }
}
