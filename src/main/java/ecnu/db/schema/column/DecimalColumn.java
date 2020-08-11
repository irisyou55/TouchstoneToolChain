package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class DecimalColumn extends AbstractColumn {
    double min;
    double max;

    public DecimalColumn() {
        super(null, ColumnType.DECIMAL);
    }

    public DecimalColumn(String columnName) {
        super(columnName, ColumnType.DECIMAL);
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        return BigDecimal.valueOf(Math.random() * (maxProbability.doubleValue() - minProbability.doubleValue()) + minProbability.doubleValue()).toString();
    }

    public BigDecimal generateData(BigDecimal probability) {
        return BigDecimal.valueOf(getMax() - getMin()).multiply(probability).add(BigDecimal.valueOf(getMin()));
    }
}
