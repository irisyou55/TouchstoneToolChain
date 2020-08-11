package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class IntColumn extends AbstractColumn {
    private int min;
    private int max;
    private int ndv;

    public IntColumn() {
        super(null, ColumnType.INTEGER);
    }

    public IntColumn(String columnName) {
        super(columnName, ColumnType.INTEGER);
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        return Integer.toString((int) (Math.random() * (maxProbability.doubleValue() - minProbability.doubleValue()) * (this.max - this.min)) + minProbability.intValue());
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    public int generateData(BigDecimal probability) {
        return BigDecimal.valueOf(getMax() - getMin()).multiply(probability).intValue() + getMin();
    }
}
