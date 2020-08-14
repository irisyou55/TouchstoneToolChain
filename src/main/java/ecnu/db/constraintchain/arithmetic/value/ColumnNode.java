package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.bucket.EqBucket;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String canonicalTableName;
    private AbstractColumn column;
    private float min;
    private float max;

    public ColumnNode() {
        super(ArithmeticNodeType.COLUMN);
    }

    public ColumnNode(AbstractColumn column) {
        super(ArithmeticNodeType.COLUMN);
        this.column = column;
    }

    public void setMinMax(float min, float max) throws TouchstoneToolChainException {
        if (min > max) {
            throw new TouchstoneToolChainException("非法的随机生成定义");
        }
        this.min = min;
        this.max = max;
    }

    public AbstractColumn getColumn() {
        return column;
    }

    public void setColumn(AbstractColumn column) {
        this.column = column;
    }

    public String getCanonicalTableName() {
        return canonicalTableName;
    }

    public void setCanonicalTableName(String canonicalTableName) {
        this.canonicalTableName = canonicalTableName;
    }

    @Override
    public float[] getVector() {
        List<EqBucket> eqBuckets = column.getEqBuckets();
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, size = BigDecimal.valueOf(ArithmeticNode.size);
        float[] value = new float[ArithmeticNode.size];
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey()).multiply(size);
                float eqValue = Float.parseFloat(entry.getValue().getData());
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < ArithmeticNode.size; j++) {
                    value[j] = eqValue;
                }
                cumBorder = newCum;
            }
        }
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = cumBorder.intValue(); i < ArithmeticNode.size; i++) {
            value[i] = threadLocalRandom.nextFloat() * bound + min;
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            Random rand = new Random();
            float tmp;
            for (int i = ArithmeticNode.size; i > 1; i--) {
                int idx = rand.nextInt(i);
                tmp = value[i - 1];
                value[i - 1] = value[idx];
                value[idx] = tmp;
            }
        }
        return value;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", canonicalTableName, column.getColumnName());
    }
}
