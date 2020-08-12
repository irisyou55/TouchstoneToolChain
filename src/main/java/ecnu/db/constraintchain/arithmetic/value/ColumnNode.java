package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.bucket.EqBucket;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
        TreeMap<Double, Float> cumulativeProbability2Param = new TreeMap<>();
        BigDecimal cumProbability = BigDecimal.ZERO;
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                cumProbability = cumProbability.add(entry.getKey());
                cumulativeProbability2Param.put(cumProbability.doubleValue(), Float.parseFloat(entry.getValue().getData()));
            }
        }
        int size = ArithmeticNode.size;
        float[] value = new float[size];
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            double probability = 1 - threadLocalRandom.nextDouble();
            Map.Entry<Double, Float> entry = cumulativeProbability2Param.ceilingEntry(probability);
            if (entry != null) {
                value[i] = entry.getValue();
            } else {
                value[i] = threadLocalRandom.nextFloat() * bound + min;
            }
        }
        return value;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", canonicalTableName, column.getColumnName());
    }
}
