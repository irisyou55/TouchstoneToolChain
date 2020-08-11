package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class BoolColumn extends AbstractColumn {
    private BigDecimal trueProbability;

    public BoolColumn() {
        super(null, ColumnType.BOOL);
    }

    public BoolColumn(String columnName) {
        super(columnName, ColumnType.BOOL);
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        BigDecimal probability = BigDecimal.valueOf(Math.random() * (maxProbability.subtract(minProbability).doubleValue())).add(minProbability);
        return Boolean.toString(probability.doubleValue()  > 0.5);
    }
}
